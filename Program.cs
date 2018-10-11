using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Repro
{
    class Program
    {
        public static async Task Main()
        {
//            if (GC.TryStartNoGCRegion(8_000_000_000, 8_000_000_000) == false)
//              Console.WriteLine("Failed To Suppress GC");

            Console.WriteLine("Starting...");
            byte[] filebytes = File.ReadAllBytes("/home/cesar/Sources/tmpcommand.txt");

            BatchRequestParser.Expected = filebytes;
            string md5 = Convert.ToBase64String(MD5.Create().ComputeHash(filebytes));

            for (int bl = 0; bl < 10000000; bl++)
            {
                // if (bl % 10 == 0)
                {
                    Console.WriteLine("\rbl =  " + bl + "  ...  ");
                    Console.Out.Flush();
                }

                const int numOfTasks = 12;
                Task[] tasks = new Task[numOfTasks];
                for (int i = 0; i < numOfTasks; i++)
                {
                    int k = i;

                    tasks[i] = Task.Run(async () =>
                    {
                        
                            JsonOperationContext.ManagedPinnedBuffer buffer =
                                JsonOperationContext.ManagedPinnedBuffer.RawNew();
                            for (int yy = 0; yy < 300; yy++)
                            {
                                using (var context = JsonOperationContext.ShortTermSingleUse())
                                {
                                    MemoryStream ms = new MemoryStream(filebytes);

                                    using (var parser =
                                        new BatchRequestParser.ReadMany(context, ms, buffer, new CancellationToken()))
                                    {
                                        parser.Init();
                                        for (int j = 0; j < 50; j++)
                                        {

                                            {
                                                parser.MoveNext(context);
                                            }
                                        }
                                    }
                                }
                            }                      
                    });
                }

                try
                {
                    Task.WaitAll(tasks);
                }
                catch (Exception e)
                {
                    Exception ei = e;
                    while (ei != null)
                    {
                        Console.WriteLine(ei.Message);
                        ei = ei.InnerException;
                    }

                    string md5_2 = Convert.ToBase64String(MD5.Create().ComputeHash(filebytes));
                    if (md5_2 != md5)
                    {
                        Console.WriteLine("WT                    ?????!!!!");
                        Console.Out.Flush();
                    }

                    throw;
                }
            }
        }
        public class BatchRequestParser
    {
        public enum CommandType
        {
            None,
            PUT
        }
        public struct CommandData
        {
            public CommandType Type;
            public string Id;
            public string DestinationId;
            public string Name;
            public BlittableJsonReaderObject Document;
            public BlittableJsonReaderObject PatchArgs;
            public BlittableJsonReaderObject PatchIfMissingArgs;
            public LazyStringValue ChangeVector;
            public bool IdPrefixed;
            public long Index;
            public bool FromEtl;
            public bool ReturnDocument;
        }


        private static readonly CommandData[] Empty = new CommandData[0];
        private static readonly int MaxSizeOfCommandsInBatchToCache = 128;

        [ThreadStatic]
        private static Stack<CommandData[]> _cache;

        static BatchRequestParser()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () =>
            {
                _cache?.Clear();
                _cache = null;
            };
        }
        public static void ReturnBuffer(ArraySegment<CommandData> cmds)
        {
            Array.Clear(cmds.Array, cmds.Offset, cmds.Count);
            ReturnBuffer(cmds.Array);
        }

        private static void ReturnBuffer(CommandData[] cmds)
        {
            if (cmds.Length > MaxSizeOfCommandsInBatchToCache)
                return;
            if (_cache == null)
                _cache = new Stack<CommandData[]>();

            if (_cache.Count > 1024)
                return;
            _cache.Push(cmds);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe long GetLongFromStringBuffer(JsonParserState state)
        {
            return *(long*)state.StringBuffer;
        }

        public class ReadMany : IDisposable
        {
            private readonly Stream _stream;
            public readonly UnmanagedJsonParser _parser;
            private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
            private readonly JsonParserState _state;
            private readonly CancellationToken _token;

            public ReadMany(JsonOperationContext ctx, Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, CancellationToken token)
            {
                _stream = stream;
                _buffer = buffer;
                _token = token;
                _buffer.Used = _buffer.Valid = 0;
                _state = new JsonParserState();
                _parser = new UnmanagedJsonParser(ctx, _state, "bulk_docs");
            }

            public void Init()
            {
                while (_parser.Read() == false)
                    RefillParserBuffer(_stream, _buffer, _parser, _token);
                if (_state.CurrentTokenType != JsonParserToken.StartArray)
                {
                    ThrowUnexpectedToken(JsonParserToken.StartArray, _state);
                }
            }

            public void Dispose()
            {
                _parser.Dispose();
            }

            public CommandData MoveNext(JsonOperationContext ctx)
            {
//                try
//                {
//                    if (_parser.Read())
//                    {
//                        if (_state.CurrentTokenType == JsonParserToken.EndArray)
//                            return default;
//
//                        return ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, _token);
//                    }
//                }
//                catch(Exception e)
//                {
//                    Console.WriteLine("REGULAR *************");
//                    throw;                    
//                }

                try
                {
                    return MoveNextUnlikely(ctx);
                }
                catch(Exception e)
                {
                    Console.WriteLine("UNLIKELY *************");
                    throw;
                }                                                   
            }

            private CommandData MoveNextUnlikely(JsonOperationContext ctx)
            {
                {
                    while (_parser.Read() == false)
                    {
                        RefillParserBuffer(_stream, _buffer, _parser, _token);
                    }
                    
                    if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        return new CommandData { Type = CommandType.None };

                    return  ReadSingleCommand(ctx, _stream, _state, _parser, _buffer, _token);
                }
               
            }
        }


        private static CommandData ReadSingleCommand(
            JsonOperationContext ctx,
            Stream stream,
            JsonParserState state,
            UnmanagedJsonParser parser,
            JsonOperationContext.ManagedPinnedBuffer buffer,
            CancellationToken token)
        {



try{

            var commandData = new CommandData();
            if (state.CurrentTokenType != JsonParserToken.StartObject)
            {
                ThrowUnexpectedToken(JsonParserToken.StartObject, state);
            }

            while (true)
            {
                while (parser.Read() == false)
                    RefillParserBuffer(stream, buffer, parser, token);

//                Console.WriteLine(Encoding.UTF8.GetString(buffer.Buffer.Array, 0, buffer.Buffer.Count));

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                {
                    ThrowUnexpectedToken(JsonParserToken.String, state);
                }
                switch (GetPropertyType(state))
                {
                    case CommandPropertyName.Type:
                        while (parser.Read() == false)
                            RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            ThrowUnexpectedToken(JsonParserToken.String, state);
                        }
                        commandData.Type = GetCommandType(state, null);
                        break;
                    case CommandPropertyName.Id:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Id = null;
                                break;
                            case JsonParserToken.String:
                                commandData.Id = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.Name:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.Name = null;
                                break;
                            case JsonParserToken.String:
                                commandData.Name = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                    case CommandPropertyName.DestinationId:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        switch (state.CurrentTokenType)
                        {
                            case JsonParserToken.Null:
                                commandData.DestinationId = null;
                                break;
                            case JsonParserToken.String:
                                commandData.DestinationId = GetStringPropertyValue(state);
                                break;
                            default:
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                                break;
                        }
                        break;
                  
                    case CommandPropertyName.Document:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        commandData.Document =  ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        break;
           
                    case CommandPropertyName.ChangeVector:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.Null)
                        {
                            commandData.ChangeVector = null;
                        }
                        else
                        {
                            if (state.CurrentTokenType != JsonParserToken.String)
                            {
                                ThrowUnexpectedToken(JsonParserToken.String, state);
                            }

                            commandData.ChangeVector = GetLazyStringValue(ctx, state);
                        }
                        break;
                    case CommandPropertyName.Index:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType != JsonParserToken.Integer)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }
                        commandData.Index = state.Long;

                        break;
                    case CommandPropertyName.IdPrefixed:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.IdPrefixed = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.ReturnDocument:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.ReturnDocument = state.CurrentTokenType == JsonParserToken.True;
                        break;
             
                    case CommandPropertyName.FromEtl:
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);

                        if (state.CurrentTokenType != JsonParserToken.True && state.CurrentTokenType != JsonParserToken.False)
                        {
                            ThrowUnexpectedToken(JsonParserToken.True, state);
                        }

                        commandData.FromEtl = state.CurrentTokenType == JsonParserToken.True;
                        break;
                    case CommandPropertyName.NoSuchProperty:
                        // unknown command - ignore it
                        while (parser.Read() == false)
                             RefillParserBuffer(stream, buffer, parser, token);
                        if (state.CurrentTokenType == JsonParserToken.StartObject ||
                            state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                             ReadJsonObject(ctx, stream, commandData.Id, parser, state, buffer, token);
                        }
                        break;
                }
            }

            switch (commandData.Type)
            {
                case CommandType.None:
                    ThrowInvalidType();
                    break;
                case CommandType.PUT:
                    if (commandData.Document == null)
                        ThrowMissingDocumentProperty();
                    break;
            }

            return commandData;
}
catch(Exception ee)
{
    System.Console.WriteLine("HERE: " + ee);
    throw;
}
        }

        private static CommandData[] IncreaseSizeOfCommandsBuffer(int index, CommandData[] cmds)
        {
            if (cmds.Length > MaxSizeOfCommandsInBatchToCache)
            {
                Array.Resize(ref cmds, Math.Max(index + 8, cmds.Length * 2));
                return cmds;
            }

            if (_cache == null)
                _cache = new Stack<CommandData[]>();
            CommandData[] tmp = null;
            while (_cache.Count > 0)
            {
                tmp = _cache.Pop();
                if (tmp.Length > index)
                    break;
                tmp = null;
            }
            if (tmp == null)
                tmp = new CommandData[cmds.Length + 8];
            Array.Copy(cmds, 0, tmp, 0, index);
            Array.Clear(cmds, 0, cmds.Length);
            ReturnBuffer(cmds);
            cmds = tmp;
            return cmds;
        }

        private static void ThrowInvalidType()
        {
            throw new InvalidOperationException($"Command must have a valid '{nameof(CommandData.Type)}' property");
        }

        private static void ThrowMissingDocumentProperty()
        {
            throw new InvalidOperationException($"PUT command must have a '{nameof(CommandData.Document)}' property");
        }

        private static void ThrowMissingNameProperty()
        {
            throw new InvalidOperationException($"Attachment PUT command must have a '{nameof(CommandData.Name)}' property");
        }

        
        
        private static BlittableJsonReaderObject ReadJsonObject(JsonOperationContext ctx, Stream stream, string id, UnmanagedJsonParser parser,
            JsonParserState state, JsonOperationContext.ManagedPinnedBuffer buffer, CancellationToken token)
        {
            if (state.CurrentTokenType == JsonParserToken.Null)
                return null;

            BlittableJsonReaderObject reader;
            using(var sssssssss = JsonOperationContext.ShortTermSingleUse())
            using (var builder = new BlittableJsonDocumentBuilder(sssssssss,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                id, parser, state, modifier: null))
            {
                sssssssss.CachedProperties.NewDocument();
                builder.ReadNestedObject();
                while (true)
                {
                    if (builder.Read())
                        break;
                     RefillParserBuffer(stream, buffer, parser, token);
                }
                builder.FinalizeDocument();
                reader = builder.CreateReader();
                reader.NoCache = true;
                reader = reader.Clone(ctx);
            }
            return reader;
        }

        private static unsafe string GetStringPropertyValue(JsonParserState state)
        {
            return Encoding.UTF8.GetString(state.StringBuffer, state.StringSize);
        }

        private static unsafe LazyStringValue GetLazyStringValue(JsonOperationContext ctx, JsonParserState state)
        {
            return ctx.GetLazyString(Encodings.Utf8.GetString(state.StringBuffer, state.StringSize));
        }

        private enum CommandPropertyName
        {
            NoSuchProperty,
            Type,
            Id,
            Document,
            ChangeVector,
            Patch,
            PatchIfMissing,
            IdPrefixed,
            Index,
            ReturnDocument,

            #region Attachment

            Name,
            DestinationId,
            DestinationName,
            ContentType,

            #endregion

            #region Counter

            Counters,

            #endregion

            FromEtl

            // other properties are ignore (for legacy support)
        }

        private static unsafe CommandPropertyName GetPropertyType(JsonParserState state)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 2:
                    if (*(short*)state.StringBuffer != 25673)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Id;

                case 8:
                    if (*(long*)state.StringBuffer == 8318823012450529091)
                        return CommandPropertyName.Counters;
                    if (*(long*)state.StringBuffer != 8389754676633104196)
                        return CommandPropertyName.NoSuchProperty;
                    return CommandPropertyName.Document;

                case 4:
                    if (*(int*)state.StringBuffer == 1701869908)
                        return CommandPropertyName.Type;
                    if (*(int*)state.StringBuffer == 1701667150)
                        return CommandPropertyName.Name;
                    return CommandPropertyName.NoSuchProperty;

                case 5:
                    if (*(int*)state.StringBuffer == 1668571472 &&
                        state.StringBuffer[4] == (byte)'h')
                        return CommandPropertyName.Patch;
                    if (*(int*)state.StringBuffer == 1701080649 &&
                        state.StringBuffer[4] == (byte)'x')
                        return CommandPropertyName.Index;
                    return CommandPropertyName.NoSuchProperty;
                case 14:
                    if (*(int*)state.StringBuffer == 1668571472 ||
                        *(long*)(state.StringBuffer + sizeof(int)) == 7598543892411468136 ||
                        *(short*)(state.StringBuffer + sizeof(int) + sizeof(long)) == 26478)
                        return CommandPropertyName.PatchIfMissing;

                    if (*(int*)state.StringBuffer == 1970562386 ||
                        *(long*)(state.StringBuffer + sizeof(int)) == 7308626840221150834 ||
                        *(short*)(state.StringBuffer + sizeof(int) + sizeof(long)) == 29806)
                        return CommandPropertyName.ReturnDocument;

                    return CommandPropertyName.NoSuchProperty;
                case 10:
                    if (*(long*)state.StringBuffer == 8676578743001572425 &&
                        *(short*)(state.StringBuffer + sizeof(long)) == 25701)
                        return CommandPropertyName.IdPrefixed;
                    return CommandPropertyName.NoSuchProperty;

                case 11:
                    if (*(long*)state.StringBuffer == 6085610378508529475 &&
                        *(short*)(state.StringBuffer + sizeof(long)) == 28793 &&
                        state.StringBuffer[sizeof(long) + sizeof(short)] == (byte)'e')
                        return CommandPropertyName.ContentType;
                    return CommandPropertyName.NoSuchProperty;

                case 12:
                    if (*(long*)state.StringBuffer == 7302135340735752259 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1919906915)
                        return CommandPropertyName.ChangeVector;
                    return CommandPropertyName.NoSuchProperty;
                case 7:
                    if (*(int*)state.StringBuffer == 1836020294 &&
                        *(short*)(state.StringBuffer + sizeof(int)) == 29765 &&
                        state.StringBuffer[6] == (byte)'l')
                        return CommandPropertyName.FromEtl;

                    return CommandPropertyName.NoSuchProperty;
                case 13:
                    if (*(long*)state.StringBuffer == 8386105380344915268 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1231974249 &&
                        state.StringBuffer[12] == (byte)'d')
                        return CommandPropertyName.DestinationId;

                    return CommandPropertyName.NoSuchProperty;
                case 15:
                    if (*(long*)state.StringBuffer == 8386105380344915268 &&
                        *(int*)(state.StringBuffer + sizeof(long)) == 1315860329 &&
                        *(short*)(state.StringBuffer + sizeof(long) + sizeof(int)) == 28001 &&
                        state.StringBuffer[14] == (byte)'e')
                        return CommandPropertyName.DestinationName;

                    return CommandPropertyName.NoSuchProperty;
                default:
                    return CommandPropertyName.NoSuchProperty;
            }
        }

        private static unsafe CommandType GetCommandType(JsonParserState state, JsonOperationContext ctx = null)
        {
            // here we confirm that the value is matching our expectation, in order to save CPU instructions
            // we compare directly against the precomputed values
            switch (state.StringSize)
            {
                case 3:
                    if (*(short*)state.StringBuffer != 21840 ||
                        state.StringBuffer[2] != (byte)'T')
                        ThrowInvalidProperty(state, ctx);

                    return CommandType.PUT;

                default:
                    ThrowInvalidProperty(state, ctx);
                    return CommandType.None;
            }
        }

        private static void ThrowInvalidUsageOfChangeVectorWithIdentities(CommandData commandData)
        {
            throw new InvalidOperationException($"You cannot use change vector ({commandData.ChangeVector}) " +
                                                $"when using identity in the document ID ({commandData.Id}).");
        }

        private static unsafe void ThrowInvalidProperty(JsonParserState state, JsonOperationContext ctx)
        {
            throw new InvalidOperationException("Invalid property name: " +
                                                ctx?.AllocateStringValue(null, state.StringBuffer, state.StringSize));
        }

        private static void ThrowUnexpectedToken(JsonParserToken jsonParserToken, JsonParserState state)
        {
            throw new InvalidOperationException("Expected " + jsonParserToken + " , but got " + state.CurrentTokenType);
        }

public static byte[] Expected;
        public static void RefillParserBuffer(Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer, UnmanagedJsonParser parser, CancellationToken token = default)
        {
            long pos = -1;
            if(stream is MemoryStream ms)
                pos = ms.Position;
            // Although we using here WithCancellation and passing the token,
            // the stream will stay open even after the cancellation until the entire server will be disposed.
            int read;
            //lock (LockAndLock.lockobj)
            {
                read = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
            }

            if (read <= 0)
                ThrowUnexpectedEndOfStream();
            if(pos != -1 && Expected != null){
                for(var i =0; i < read; i++){
                    if(buffer.Buffer.Array[i + buffer.Buffer.Offset] != Expected[i + pos])
                    {
                        throw new Exception("Mismatch read!!!!");
                    }
                }
            }

            if (parser.BufferOffset != parser.BufferSize)
            {
                Console.WriteLine(   "really?");
            }
            parser.SetBuffer(buffer, 0, read);
        }

        private static void ThrowUnexpectedEndOfStream()
        {
            throw new EndOfStreamException();
        }

    }
    }
}
