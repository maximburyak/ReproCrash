using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow.Json.Parsing
{
    public unsafe class JsonParserState
    {
        public const int EscapePositionItemSize = 5;
        public byte* StringBuffer;
        public int StringSize;                
        public JsonParserToken CurrentTokenType;
        public JsonParserTokenContinuation Continuation;        
       
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVariableSizeInt(ref byte* dest, int value)
        {
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                *dest++ = (byte)(v | 0x80);
                v >>= 7;
            }
            *dest++ = (byte)(v);
        }            

        public int WriteEscapePositionsTo(byte* buffer)
        {            
            var originalBuffer = buffer;
            WriteVariableSizeInt(ref buffer, 0);
            return (int)(buffer - originalBuffer);
        }
    }
}
