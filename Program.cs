using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Repro
{
    class Program
    {
        public static unsafe void Main()
        {
            Console.WriteLine("Starting...");            

            for (int ss = 0; ss < 100; ss++)
            {                
                Console.WriteLine("************ " + ss + " ***********");
                Parallel.For(0, 20, a =>
                {
                    string readAllText = File.ReadAllText("tmpcommand.txt");
                    readAllText = readAllText.Replace("/", "_");
                    byte[] filebytes1 = Encoding.UTF8.GetBytes(readAllText);
                    for (int i = 0; i < 1; i++)
                    {                        
                        ParseFile(filebytes1);
                    }
                }
                );
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);                
            }
            Console.WriteLine("Done");

        }

        private unsafe static void ParseFile(byte[] filebytes1)
        {
            MemoryStream stream = new MemoryStream(filebytes1,0,filebytes1.Length, true, true);            
            var state = new JsonParserState();
            using(var ctx = new JsonOperationContext(1024* 32, 1024*128, SharedMultipleUseFlag.None))
            using (var sssssssss = JsonOperationContext.ShortTermSingleUse())
            using (var parser = new UnmanagedJsonParser(sssssssss, state, "ourtest"))
            {
                ReadNextToken(parser, stream, true);

                if (state.CurrentTokenType != JsonParserToken.StartArray)
                {
                    Console.WriteLine("Not array");
                    return;
                }

                while (true)
                {
                    ReadNextToken(parser, stream);

                    if (state.CurrentTokenType == JsonParserToken.EndArray)
                        break;

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                    {
                        Console.WriteLine("Not an object: " + state.CurrentTokenType);
                        return;
                    }

                    while (true)
                    {

                        if (state.CurrentTokenType == JsonParserToken.EndObject)
                            break;


                        ReadNextToken(parser, stream);

                        if (state.CurrentTokenType != JsonParserToken.String)
                        {
                            Console.WriteLine("Missing prop");
                            return;
                        }

                        switch (Encoding.UTF8.GetString(state.StringBuffer, state.StringSize))
                        {
                            case "Type":
                            case "Id":
                                ReadNextToken(parser, stream);

                                if (state.CurrentTokenType != JsonParserToken.String)
                                {
                                    Console.WriteLine("Missing val");
                                    return;
                                }

                                break;
                            case "Document":
                                ReadNextToken(parser, stream);

                                if (state.CurrentTokenType != JsonParserToken.StartObject)
                                {
                                    Console.WriteLine("Missing document");
                                    return;
                                }

                                ReadJson(parser, stream, state, sssssssss);

                                ReadNextToken(parser, stream);
                                if (state.CurrentTokenType != JsonParserToken.EndObject)
                                {
                                    Console.WriteLine("Document not closing cmd");
                                    return;
                                }

                                break;
                            default:
                                Console.WriteLine("Bad prop");
                                return;
                        }
                    }
                }
            }
        }

        
        private static void ReadJson(UnmanagedJsonParser parser, MemoryStream stream, JsonParserState state, JsonOperationContext sssssssss)
        {
//            var endObj = 1;
//            while (endObj != 0)
//            {
//                ReadNextToken(parser, stream);
//                if (state.CurrentTokenType == JsonParserToken.EndObject)
//                {
//                    endObj--;
//                }
//
//                if (state.CurrentTokenType == JsonParserToken.StartObject)
//                {
//                    endObj++;
//                }
//            }

            using (var builder = new BlittableJsonDocumentBuilder(sssssssss,
                BlittableJsonDocumentBuilder.UsageMode.ToDisk,
                "users/1", parser, state))
            {
                sssssssss.CachedProperties = new CachedProperties(JsonOperationContext.ShortTermSingleUse());
                builder.ReadNestedObject();

                while (true)
                {
                    if (builder.Read())
                        break;


                    RefillParserBuffer(stream,  parser, new CancellationToken());// .Wait();
                }

                // builder.FinalizeDocument();
            }
        }

        private static void ReadNextToken(UnmanagedJsonParser parser, MemoryStream stream, bool isFirst = false)
        {
            if (parser.Read() == false)
            {
                RefillParserBuffer(stream, parser, CancellationToken.None); //.Wait();
                if (parser.Read() == false)
                {
                    Console.WriteLine("OMASMASDAS");
                    Environment.Exit(2222);
                }
            }
            else if (isFirst)
            {
                Console.WriteLine("ERRRRRRRRR");
                Console.Out.Flush();
            }
        }
        
        public unsafe static void RefillParserBuffer(Stream stream, UnmanagedJsonParser parser, CancellationToken token = default)
        {
            // Although we using here WithCancellation and passing the token,
            // the stream will stay open even after the cancellation until the entire server will be disposed.

            const int size = 8 * 1024 * 1024;
            var arr = new byte[size];
            
            
            // var read = stream.Read(buffer.Buffer.Array.Buffer.Offset.Buffer.Count); // .WithCancellation(token);
            var read = stream.Read(arr, 0, size); // .WithCancellation(token);
            if (read == 0)
                throw new EndOfStreamException();

            var pArr = (byte *)Marshal.AllocHGlobal(read);
            
            for (int i = 0; i < read; i++)
            {
                *(pArr + i) = arr[i];
            }
            
            
            // File.WriteAllBytes("/tmp/aaa" + Guid.NewGuid(), arr);            
            // parser.SetBuffer(buffer, 0, read);
            parser.SetBuffer(pArr, read);
                        
        }

    }
}
	
