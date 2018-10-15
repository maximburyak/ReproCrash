using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform.Posix;

namespace Sparrow
{
    public static unsafe class Memory
    {      

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            return Syscall.Compare((byte*)p1, (byte*)p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(byte* p1, byte* p2, int size, out int position)
        {
            for (position = 0; position < size; position++)
            {
                if (p1[position] != p2[position])
                    return p1[position] - p2[position];
            }

            return 0;
        }

        /// <summary>
        /// Bulk copy is optimized to handle copy operations where n is statistically big. While it will use a faster copy operation for 
        /// small amounts of memory, when you have smaller than 2048 bytes calls (depending on the target CPU) it will always be
        /// faster to call .Copy() directly.
        /// </summary>
        
        private static void BulkCopy(void* dest, void* src, long n)
        {
            Syscall.Copy((byte*)dest, (byte*)src, n);            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, uint n)
        {
            Unsafe.CopyBlock(dest, src, n);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, int n)
        {
            Unsafe.CopyBlock(dest, src, (uint)n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, long n)
        {
            if (n < uint.MaxValue)
            {
                Unsafe.CopyBlock(dest, src, (uint)n); // Common code-path
                return;
            }

            BulkCopy(dest, src, n);
        }
    }
}
