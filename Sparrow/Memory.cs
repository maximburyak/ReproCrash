using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public static unsafe class Memory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, int n)
        {
            Unsafe.CopyBlock(dest, src, (uint)n);
        }
      
    }
}
