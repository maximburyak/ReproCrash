using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Json.Parsing
{
    public unsafe class UnmanagedJsonParser : IJsonParser
    {
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] Utf8Preamble = Encoding.UTF8.GetPreamble();
        private readonly string _debugTag;
        private UnmanagedWriteBuffer _unmanagedWriteBuffer;        
        private int _currentStrStart;
        private readonly JsonOperationContext _ctx;
        private readonly JsonParserState _state;
        private uint _pos;
        private uint _bufSize;
        private int _line = 1;
        private uint _charPos = 1;
        private byte* _inputBuffer;
        private int _prevEscapePosition;
        private byte _currentQuote;
        private byte[] _expectedTokenBuffer;
        private int _expectedTokenBufferPosition;
        private string _expectedTokenString;       
        private bool _escapeMode;
        private bool _maybeBeforePreamble = true;    

        static UnmanagedJsonParser()
        {
            ParseStringTable = new byte[255];
            ParseStringTable['r'] = (byte)'\r';
            ParseStringTable['n'] = (byte)'\n';
            ParseStringTable['b'] = (byte)'\b';
            ParseStringTable['f'] = (byte)'\f';
            ParseStringTable['t'] = (byte)'\t';
            ParseStringTable['"'] = (byte)'"';
            ParseStringTable['\\'] = (byte)'\\';
            ParseStringTable['/'] = (byte)'/';
            ParseStringTable['\n'] = Unlikely;
            ParseStringTable['\r'] = Unlikely;
            ParseStringTable['u'] = Unlikely;

        }

        public UnmanagedJsonParser(JsonOperationContext ctx, JsonParserState state, string debugTag)
        {
            _ctx = ctx;
            _state = state;
            _debugTag = debugTag;
            _unmanagedWriteBuffer = new UnmanagedWriteBuffer(ctx, ctx.GetMemory(1024*16));
        }     

        public void SetBuffer(byte* inputBuffer, int size)
        {
            _inputBuffer = inputBuffer;
            _bufSize = (uint)size;
            _pos = 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            var state = _state;
            if (state.Continuation != JsonParserTokenContinuation.None || _maybeBeforePreamble)
                goto ReadContinuation;

MainLoop:

            byte b;
            byte* currentBuffer = _inputBuffer;
            uint bufferSize = _bufSize;
            uint pos = _pos;
            while (true)
            {
                if (pos >= bufferSize)
                    goto ReturnFalse;

                b = currentBuffer[pos];
                pos++;
                _charPos++;

                if (b == ':' || b == ',')
                {                    
                    if (state.CurrentTokenType == JsonParserToken.Separator || state.CurrentTokenType == JsonParserToken.StartObject || state.CurrentTokenType == JsonParserToken.StartArray)
                        goto Error;

                    state.CurrentTokenType = JsonParserToken.Separator;
                    continue;
                }

                if (b == '\'' || b == '"')
                    goto ParseString; // PERF: Avoid very lengthy method here; as we are going to return anyways.
                
                if (b == '{')
                {
                    state.CurrentTokenType = JsonParserToken.StartObject;
                    goto ReturnTrue;
                }

                if (b == '}')
                {
                    state.CurrentTokenType = JsonParserToken.EndObject;
                    goto ReturnTrue;
                }
                if (b == '[')
                {
                    state.CurrentTokenType = JsonParserToken.StartArray;
                    goto ReturnTrue;
                }
                if (b == ']')
                {
                    state.CurrentTokenType = JsonParserToken.EndArray;
                    goto ReturnTrue;
                }

                bool couldRead;
                if (!ReadUnlikely(b, ref pos, out couldRead))
                    continue; // We can only continue here, if there is a failure to parse, we will throw inside ReadUnlikely.

                if (couldRead)
                    goto ReturnTrue;
                goto ReturnFalse;
            }

ParseString:
            {
                state.EscapePositions.Clear();
                _unmanagedWriteBuffer.Clear();
                _prevEscapePosition = 0;
                _currentQuote = b;
                state.CurrentTokenType = JsonParserToken.String;
                if (ParseString(ref pos) == false)
                {
                    state.Continuation = JsonParserTokenContinuation.PartialString;
                    goto ReturnFalse;
                }
                _unmanagedWriteBuffer.EnsureSingleChunk(state);
                goto ReturnTrue;
            }

Error:
            ThrowCannotHaveCharInThisPosition(b);

ReturnTrue:
            _pos = pos;
            return true;

ReturnFalse:
            _pos = pos;
            return false;


ReadContinuation: // PERF: This is a "manual procedure"            

            state.Continuation = JsonParserTokenContinuation.None;
            if (_maybeBeforePreamble)
            {                   
                System.Console.Out.Flush();
                if (ReadMaybeBeforePreamble() == false)
                    return false;
            }

            goto MainLoop;
        }     

        private bool ReadUnlikely(byte b, ref uint pos, out bool couldRead)
        {            
            couldRead = false;
            switch (b)
            {
                case (byte)'\r':
                    {
                        if (pos >= _bufSize)
                        {
                            return true;
                        }
                        if (_inputBuffer[pos] == (byte)'\n')
                        {
                            return false;
                        }
                        goto case (byte)'\n';
                    }

                case (byte)'\n':
                    {
                        _line++;
                        _charPos = 1;
                        return false;
                    }               

                case (byte)'n':
                    {                        
                        _state.CurrentTokenType = JsonParserToken.Null;
                        _expectedTokenBuffer = NullBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "null";
                        if (EnsureRestOfToken(ref pos) == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialNull;
                            return true;
                        }

                        couldRead = true;
                        return true;
                    }               
            }            

            ThrowCannotHaveCharInThisPosition(b);
            return false;
        }      

        private void ThrowCannotHaveCharInThisPosition(byte b)
        {
            ThrowException("Cannot have a '" + (char)b + "' in this position");
        }

        private bool ReadMaybeBeforePreamble()
        {
            if (_pos >= _bufSize)
            {
                return false;
            }

            if (_inputBuffer[_pos] == Utf8Preamble[0])
            {
                _pos++;
                _expectedTokenBuffer = Utf8Preamble;
                _expectedTokenBufferPosition = 1;
                _expectedTokenString = "UTF8 Preamble";
                if (EnsureRestOfToken(ref _pos) == false)
                {
                    _state.Continuation = JsonParserTokenContinuation.PartialPreamble;
                    return false;
                }
            }
            else
            {
                _maybeBeforePreamble = false;
            }
            return true;
        }     

        private bool EnsureRestOfToken(ref uint pos)
        {
            uint bufferSize = _bufSize;
            byte* inputBuffer = _inputBuffer;
            byte[] expectedTokenBuffer = _expectedTokenBuffer;
            for (int i = _expectedTokenBufferPosition; i < expectedTokenBuffer.Length; i++)
            {
                if (pos >= bufferSize)
                    return false;

                if (inputBuffer[pos++] != expectedTokenBuffer[i])
                    ThrowException("Invalid token found, expected: " + _expectedTokenString);

                _expectedTokenBufferPosition++;
                _charPos++;
            }
            return true;
        }
        
        private const byte Unlikely = 1;
        private static readonly byte[] ParseStringTable;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ParseString(ref uint currentPos)
        {
            byte* currentBuffer = _inputBuffer;
            byte[] parseStringTable = ParseStringTable;

            uint bufferSize = _bufSize;

            while (true)
            {
                _currentStrStart = (int)currentPos;

                while (currentPos < bufferSize)
                {
                    byte b = currentBuffer[currentPos];
                    currentPos++;
                    _charPos++;

                    if (_escapeMode == false)
                    {
                        // PERF: Early escape to avoid jumping around in the code layout.
                        if (b != _currentQuote && b != (byte)'\\')
                            continue;

                        _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, (int)currentPos - _currentStrStart - 1 /* don't include the escape or the last quote */);

                        if (b == _currentQuote)
                            goto ReturnTrue;

                        // Then it is '\\'
                        _escapeMode = true;
                        _currentStrStart = (int)currentPos;
                    }
                    else
                    {
                        _currentStrStart++;
                        _escapeMode = false;
                        _charPos++;
                        if (b != (byte)'u' && b != (byte)'/')
                        {
                            _state.EscapePositions.Add(_unmanagedWriteBuffer.SizeInBytes - _prevEscapePosition);
                            _prevEscapePosition = _unmanagedWriteBuffer.SizeInBytes + 1;
                        }

                        byte op = parseStringTable[b];
                        if (op > Unlikely)
                        {
                            // We have a known substitution to apply
                            _unmanagedWriteBuffer.WriteByte(op);
                        }
                        else if (b == (byte)'\n')
                        {
                            _line++;
                            _charPos = 1;
                        }
                        else if (b == (byte)'\r')
                        {
                            if (currentPos >= bufferSize)
                                goto ReturnFalse;

                            _line++;
                            _charPos = 1;
                            if (currentPos >= bufferSize)
                                goto ReturnFalse;

                            if (currentBuffer[currentPos] == (byte)'\n')
                                currentPos++; // consume the \,\r,\n
                        }
                        else if (b == (byte)'u')
                        {
                            if (ParseUnicodeValue(ref currentPos) == false)
                                goto ReturnFalse;
                        }
                        else
                        {
                            ThrowInvalidEscapeChar(b);
                        }
                    }
                }

                // copy the buffer to the native code, then refill
                _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, (int)currentPos - _currentStrStart);

                if (currentPos >= bufferSize)
                    goto ReturnFalse;
            }


ReturnTrue:
            return true;

ReturnFalse:
            return false;
        }

        private static void ThrowInvalidEscapeChar(byte b)
        {
            throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
        }


        private bool ParseUnicodeValue(ref uint pos)
        {
            byte b;
            int val = 0;

            byte* inputBuffer = _inputBuffer;
            uint bufferSize = _bufSize;
            for (int i = 0; i < 4; i++)
            {
                if (pos >= bufferSize)
                    return false;

                b = inputBuffer[pos];
                pos++;
                _currentStrStart++;

                if (b >= (byte)'0' && b <= (byte)'9')
                {
                    val = (val << 4) | (b - (byte)'0');
                }
                else if (b >= 'a' && b <= (byte)'f')
                {
                    val = (val << 4) | (10 + (b - (byte)'a'));
                }
                else if (b >= 'A' && b <= (byte)'F')
                {
                    val = (val << 4) | (10 + (b - (byte)'A'));
                }
                else
                {
                    ThrowException("Invalid hex value , numeric value is: " + b);
                }
            }
            WriteUnicodeCharacterToStringBuffer(val);
            return true;
        }

        private void WriteUnicodeCharacterToStringBuffer(int val)
        {
            var smallBuffer = stackalloc byte[8];
            var chars = stackalloc char[1];
            try
            {
                chars[0] = Convert.ToChar(val);
            }
            catch (Exception e)
            {
                throw new FormatException("Could not convert value " + val + " to char", e);
            }
            var byteCount = Encoding.UTF8.GetBytes(chars, 1, smallBuffer, 8);
            _unmanagedWriteBuffer.Write(smallBuffer, byteCount);
        }
        protected void ThrowException(string message, Exception inner = null)
        {
            throw new InvalidDataException($"{message} at {GenerateErrorState()}", inner);
        }

        public void Dispose()
        {
            _unmanagedWriteBuffer.Dispose();
        }

        public string GenerateErrorState()
        {
            var s = Encoding.UTF8.GetString(_inputBuffer, (int)_bufSize);
            return " (" + _line + "," + _charPos + ") around: " + s;
        }
    }
}
