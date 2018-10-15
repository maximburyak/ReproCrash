using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if MEM_GUARD
using Sparrow.Platform;
#endif

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData Allocate(int size)
        {
            var allocatedMemoryData = new AllocatedMemoryData
            {
                Address = (byte*) Marshal.AllocHGlobal(size),
                SizeInBytes = size
            };
            return allocatedMemoryData;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(AllocatedMemoryData allocation)
        {
      //      Marshal.FreeHGlobal((IntPtr)allocation.Address);
        }

    }

    public unsafe class AllocatedMemoryData
    {
        public int SizeInBytes;
        public int ContextGeneration;

        public JsonOperationContext Parent;
        public byte* Address;


    }
}
