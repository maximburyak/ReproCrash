using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Threading;
#if MEM_GUARD
using Sparrow.Platform;
#endif

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator : IDisposable
    {
        public ArenaMemoryAllocator()
        {            
        }

        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData Allocate(int size)
        {
            return new AllocatedMemoryData
            {
                Address = (byte*) Marshal.AllocHGlobal(size),
                SizeInBytes = size
            };
        }        

        ~ArenaMemoryAllocator()
        {
            try
            {
                Dispose();
            }
            catch (ObjectDisposedException)
            {
                // This is expected, we might be calling the finalizer on an object that
                // was already disposed, we don't want to error here because of this
            }
        }

        public void Dispose()
        {
          
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(AllocatedMemoryData allocation)
        {
            Marshal.FreeHGlobal((IntPtr)allocation.Address);
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
