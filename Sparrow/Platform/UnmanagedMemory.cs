using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow
{
    public static unsafe class UnmanagedMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IntPtr Copy(byte* dest, byte* src, long count)
        {
            Debug.Assert(count >= 0);
            return Syscall.Copy(dest, src, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(byte* b1, byte* b2, long count)
        {
            Debug.Assert(count >= 0);
            return Syscall.Compare(b1, b2, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Move(byte* dest, byte* src, long count)
        {
            Debug.Assert(count >= 0);
            return    Syscall.Move(dest, src, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IntPtr Set(byte* dest, int c, long count)
        {
            Debug.Assert(count >= 0);
                return Syscall.Set(dest, c, count);
        }
    }
}
