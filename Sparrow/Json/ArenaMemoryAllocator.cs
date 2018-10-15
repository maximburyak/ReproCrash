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
        private const int MaxArenaSize = 1024 * 1024 * 1024;

        private byte* _ptrStart;
        private byte* _ptrCurrent;

        private long _allocated;
        private long _used;


        private struct FreeSection
        {
#pragma warning disable 649
            public FreeSection* Previous;
            public int SizeInBytes;
#pragma warning restore 649
        }

        private readonly FreeSection*[] _freed = new FreeSection*[32];

        private readonly SingleUseFlag _isDisposed = new SingleUseFlag();
        private readonly int _initialSize;

        public long TotalUsed;

        public bool AvoidOverAllocation;

        private readonly SharedMultipleUseFlag _lowMemoryFlag;

        public long Allocated
        {
            get
            {
                var totalAllocation = _allocated;
              
                return totalAllocation;
            }
        }

        public ArenaMemoryAllocator(SharedMultipleUseFlag lowMemoryFlag, int initialSize = 1024 * 1024)
        {
            _initialSize = initialSize;
            _allocated = initialSize;
            _used = 0;
            TotalUsed = 0;
            _lowMemoryFlag = lowMemoryFlag;
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

        public void RenewArena()
        {
         
        }

        public void ResetArena()
        {
         
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

#if MEM_GUARD_STACK || TRACK_ALLOCATED_MEMORY_DATA
        public string AllocatedBy = Environment.StackTrace;
        public string FreedBy;
#endif

#if !DEBUG
        public byte* Address;
#else
        public bool IsLongLived;
        public bool IsReturned;
        private byte* _address;
        public byte* Address
        {
            get
            {
                if (IsLongLived == false &&
                    Parent != null &&
                    ContextGeneration != Parent.Generation ||
                    IsReturned)
                    ThrowObjectDisposedException();

                return _address;
            }
            set
            {
                if (IsLongLived == false &&
                    Parent != null &&
                    ContextGeneration != Parent.Generation ||
                    IsReturned)
                    ThrowObjectDisposedException();

                _address = value;
            }
        }

        private void ThrowObjectDisposedException()
        {
           throw new ObjectDisposedException(nameof(AllocatedMemoryData));
        }
#endif

    }
}
