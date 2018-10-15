using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public sealed class BlittableJsonDocumentBuilder : IDisposable
    {

        private static readonly string UnderscoreSegment = "_";

        private readonly Stack<BuildingState> _continuationState = new Stack<BuildingState>();

        private readonly IJsonParser _reader;
        private readonly JsonParserState _state;

        private WriteToken _writeToken;

        public BlittableJsonDocumentBuilder(JsonParserState state, IJsonParser reader)
        {
            _state = state;
            _reader = reader;         
        }

        public BlittableJsonDocumentBuilder(IJsonParser reader, JsonParserState state) : this(state, reader)
        {
            Renew();
        }

        public void Reset()
        {
            _continuationState.Clear();
            _writeToken = default(WriteToken);
        }

        public void Renew()
        {
            _writeToken = default(WriteToken);
            _continuationState.Clear();                      
        }


        public void ReadNestedObject()
        {
            _continuationState.Push(new BuildingState(ContinuationState.ReadObject));
        }

        public void Dispose()
        {
        }

        private bool ReadInternal<TWriteStrategy>() where TWriteStrategy : IWriteStrategy
        {
            Stack<BuildingState> continuationState;
            BuildingState currentState;
            IJsonParser reader;
            JsonParserState state;
            // lock (LockClass.lockobj)
            {
                continuationState = _continuationState;
                currentState = continuationState.Pop();
                reader = _reader;
                state = _state;
            }

            while (true)
            {
                // lock (LockClass.lockobj)
                {
                    switch (currentState.State)
                    {
                        case ContinuationState.ReadObjectDocument:
                            if (reader.Read() == false)
                            {
                                continuationState.Push(currentState);
                                return false;
                            }

                            currentState.State = ContinuationState.ReadObject;
                            continue;
                        case ContinuationState.ReadArrayDocument:
                            if (reader.Read() == false)
                            {
                                continuationState.Push(currentState);
                                return false;
                            }

                            currentState.Properties = new List<PropertyTag>(); 
                            currentState.State = ContinuationState.CompleteDocumentArray;
                            continuationState.Push(currentState);
                            currentState = new BuildingState(ContinuationState.ReadArray);
                            continue;

                        case ContinuationState.CompleteDocumentArray:
                            currentState.Properties[0] = new PropertyTag(
                                type: (byte)_writeToken.WrittenToken,
                                property: currentState.Properties[0].PropertyId,
                                position: _writeToken.ValuePos
                            );

                            // Register property position, name id (PropertyId) and type (object type and metadata)
                            return true;

                        case ContinuationState.ReadObject:
                            if (state.CurrentTokenType == JsonParserToken.StartObject)
                            {
                                currentState.State = ContinuationState.ReadPropertyName;
                                currentState.Properties = new List<PropertyTag>(); 
                                continue;
                            }

                            goto ErrorExpectedStartOfObject;

                        case ContinuationState.ReadArray:
                            if (state.CurrentTokenType == JsonParserToken.StartArray)
                            {
                                currentState.Types = new List<BlittableJsonToken>();
                                currentState.Positions = new List<int>(); 
                                currentState.State = ContinuationState.ReadArrayValue;
                                continue;
                            }

                            goto ErrorExpectedStartOfArray;

                        case ContinuationState.ReadArrayValue:
                            if (reader.Read() == false)
                            {
                                continuationState.Push(currentState);
                                return false;
                            }

                            if (state.CurrentTokenType == JsonParserToken.EndArray)
                            {
                                currentState.State = ContinuationState.CompleteArray;
                                continue;
                            }

                            currentState.State = ContinuationState.CompleteArrayValue;
                            continuationState.Push(currentState);
                            currentState = new BuildingState(ContinuationState.ReadValue);
                            continue;

                        case ContinuationState.CompleteArrayValue:
                            currentState.Types.Add(_writeToken.WrittenToken);
                            currentState.Positions.Add(_writeToken.ValuePos);
                            currentState.State = ContinuationState.ReadArrayValue;
                            continue;

                        case ContinuationState.CompleteArray:
                            var arrayToken = BlittableJsonToken.StartArray;

                            currentState = continuationState.Pop();
                            continue;

                        case ContinuationState.ReadPropertyName:
                            if (ReadMaybeModifiedPropertyName() == false)
                            {
                                continuationState.Push(currentState);
                                return false;
                            }

                            if (state.CurrentTokenType == JsonParserToken.EndObject)
                            {
                                if (continuationState.Count == 0)
                                    return true;

                                currentState = continuationState.Pop();
                                continue;
                            }

                            if (state.CurrentTokenType != JsonParserToken.String)
                                goto ErrorExpectedProperty;
                          
                            currentState.State = ContinuationState.ReadPropertyValue;
                            continue;
                        case ContinuationState.ReadPropertyValue:          
                            if (reader.Read() == false)
                            {
                                continuationState.Push(currentState);
                                return false;
                            }

                            currentState.State = ContinuationState.CompleteReadingPropertyValue;
                            continuationState.Push(currentState);
                            currentState = new BuildingState(ContinuationState.ReadValue);
                            continue;
                        case ContinuationState.CompleteReadingPropertyValue:
                            // Register property position, name id (PropertyId) and type (object type and metadata)
                            currentState.Properties.Add(new PropertyTag(
                                position: _writeToken.ValuePos,
                                type: (byte)_writeToken.WrittenToken,
                                property: currentState.CurrentPropertyId));

                            currentState.State = ContinuationState.ReadPropertyName;
                            continue;
                        case ContinuationState.ReadValue:
                            ReadJsonValue<TWriteStrategy>();
                            currentState = _continuationState.Pop();
                            break;
                    }
                }
            }

          
            ErrorExpectedProperty:
            ThrowExpectedProperty();
            ErrorExpectedStartOfObject:
            ThrowExpectedStartOfObject();
            ErrorExpectedStartOfArray:
            ThrowExpectedStartOfArray();
            return false; // Will never execute.            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            if (_continuationState.Count == 0)
                return false; //nothing to do

            return ReadInternal<WriteFull>();
        }

        private bool ReadMaybeModifiedPropertyName()
        {
            return _reader.Read();
        }

        private void ThrowExpectedProperty()
        {
            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfArray()
        {
            throw new InvalidDataException("Expected start of array, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfObject()
        {
            throw new InvalidDataException("Expected start of object, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private interface IWriteStrategy { }
        private struct WriteFull : IWriteStrategy { }
        private struct WriteNone : IWriteStrategy { }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void ReadJsonValue<TWriteStrategy>() where TWriteStrategy : IWriteStrategy
        {
            int start =1;
            JsonParserToken current = _state.CurrentTokenType;
            if (current == JsonParserToken.String)
            {
                BlittableJsonToken stringToken = BlittableJsonToken.Null;
                if (typeof(TWriteStrategy) == typeof(WriteNone))
                {
                //    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, _state.EscapePositions, out stringToken, _mode, _state.CompressedSize);
                }
                else // WriteFull
                {
                    if (_state.EscapePositions.Count == 0 && _state.CompressedSize == null  && _state.StringSize < 128)
                    {
                     //   start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);
                        stringToken = BlittableJsonToken.String;
                    }
                    else
                    {
                  //      start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, _state.EscapePositions, out stringToken, _mode, _state.CompressedSize);
                    }                        
                }
                _state.CompressedSize = null;
                _writeToken = new WriteToken(start, stringToken);
            }
            else if (current == JsonParserToken.Integer)
            {
               // start = _writer.WriteValue(_state.Long);
                _writeToken = new WriteToken(start, BlittableJsonToken.Integer);
            }
            else if (current == JsonParserToken.StartObject)
            {
                _continuationState.Push(new BuildingState(ContinuationState.ReadObject));
            }
            else if (current != JsonParserToken.EndObject)
            { 
                ReadJsonValueUnlikely<TWriteStrategy>(current);
            }       
        }

        private unsafe void ReadJsonValueUnlikely<TWriteStrategy>(JsonParserToken current) where TWriteStrategy : IWriteStrategy
        {
            int start =1;
            switch (current)
            {
                case JsonParserToken.StartArray:
                    _continuationState.Push(new BuildingState(ContinuationState.ReadArray));
                    return;
                case JsonParserToken.Float:

                    return;
                case JsonParserToken.True:
                case JsonParserToken.False:

                    return;
                case JsonParserToken.Null:
                    return;
            }

            ThrowExpectedValue(current);
        }

        private void ThrowExpectedValue(JsonParserToken token)
        {
            throw new InvalidDataException("Expected a value, but got " + token);
        }


        public enum ContinuationState
        {
            ReadPropertyName,
            ReadPropertyValue,
            ReadArray,
            ReadArrayValue,
            ReadObject,
            ReadValue,
            CompleteReadingPropertyValue,
            ReadObjectDocument,
            ReadArrayDocument,
            CompleteDocumentArray,
            CompleteArray,
            CompleteArrayValue
        }

        public struct BuildingState
        {
            public ContinuationState State;
            public int MaxPropertyId;
            public int? CurrentPropertyId;
            public List<PropertyTag> Properties;
            public List<BlittableJsonToken> Types;
            public List<int> Positions;
            public long FirstWrite;

            public BuildingState(ContinuationState state)
            {
                State = state;
                MaxPropertyId = 0;
                CurrentPropertyId = null;
                Properties = null;
                Types = null;
                Positions = null;
                FirstWrite = 0;
            }
        }


        public struct PropertyTag
        {
            public int Position;

            public int? PropertyId;
            public byte Type;

            public PropertyTag(byte type, int? property, int position)
            {
                Type = type;
                PropertyId = property;
                Position = position;
            }
        }        

        public struct WriteToken
        {
            public int ValuePos;
            public BlittableJsonToken WrittenToken;

            public WriteToken(int position, BlittableJsonToken token)
            {
                ValuePos = position;
                WrittenToken = token;
            }
        }   

        public override string ToString()
        {
            return "Building json";
        }    
    }
}
