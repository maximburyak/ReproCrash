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
        private long _allocated;

        public long Allocated
        {
            get
            {
                var totalAllocation = _allocated;
              
                return totalAllocation;
            }
        }

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

        public class IntPtrComarer : IComparer<IntPtr>
        {
            public static IntPtrComarer Instance = new IntPtrComarer();

            public int Compare(IntPtr x, IntPtr y)
            {
                return Math.Sign((x.ToInt64() - y.ToInt64()));
            }
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
