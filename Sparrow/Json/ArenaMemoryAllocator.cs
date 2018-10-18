using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Threading;


namespace Sparrow.Json
{
    public unsafe class AllocatedMemoryData
    {
        public AllocatedMemoryData(int size)
        {
            Address = (byte*) Marshal.AllocHGlobal(size);
            SizeInBytes = size;
        }
        public int SizeInBytes;
        public int ContextGeneration;
        public JsonOperationContext Parent;
        public byte* Address;
    }
}
