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

        public static void FindEscapePositionsIn(List<int> buffer, byte* str, int len, int previousComputedMaxSize)
        {
            buffer.Clear();
            if (previousComputedMaxSize == EscapePositionItemSize)
            {
                // if the value is 5, then we got no escape positions, see: FindEscapePositionsMaxSize
                // and we don't have to do any work
                return;
            }

            var lastEscape = 0;
            for (int i = 0; i < len; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 13 => '\r' => 0000 1101
                // 10 => '\n' => 0000 1010
                // 12 => '\f' => 0000 1100
                // 34 => '\\' => 0010 0010
                // 92 =>  '"' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    buffer.Add(i - lastEscape);
                    lastEscape = i + 1;
                }
            }
        }

        public int WriteEscapePositionsTo(byte* buffer)
        {            
            var originalBuffer = buffer;
            WriteVariableSizeInt(ref buffer, 0);
            return (int)(buffer - originalBuffer);
        }
    }
}
