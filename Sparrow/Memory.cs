using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public static unsafe class Memory
    {
        public const int CompareInlineVsCallThreshold = 256;

        public static int Compare(byte* p1, byte* p2, int size)
        {
            return CompareInline(p1, p2, size);
        }

        public static int Compare(byte* p1, byte* p2, int size, out int position)
        {
            return CompareInline(p1, p2, size, out position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            return UnmanagedMemory.Compare((byte*)p1, (byte*)p2, size);
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
            UnmanagedMemory.Copy((byte*)dest, (byte*)src, n);            
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, uint n)
        {
            Unsafe.InitBlock(dest, value, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, int n)
        {
            Unsafe.InitBlock(dest, value, (uint)n);
        }

        public static void Set(byte* dest, byte value, long n)
        {
            SetInline(dest, value, n);
        }

        /// <summary>
        /// Set is optimized to handle copy operations where n is statistically small.       
        /// </summary>
        /// <remarks>This is a forced inline version, use with care.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetInline(byte* dest, byte value, long n)
        {
            if (n == 0)
                goto Finish;

            if (n < int.MaxValue)
            {
                Unsafe.InitBlock(dest, value, (uint)n);
            }
            else
            {
                UnmanagedMemory.Set(dest, value, n);
            }

            Finish:
            ;
        }
    }
}
