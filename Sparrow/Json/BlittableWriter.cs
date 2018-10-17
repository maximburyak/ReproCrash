using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public sealed class BlittableWriter<TWriter> : IDisposable
        where TWriter : struct
    {
        private readonly JsonOperationContext _context;
        //private TWriter _unmanagedWriteBuffer;
        private AllocatedMemoryData _compressionBuffer;
        private AllocatedMemoryData _innerBuffer;    


        static BlittableWriter()
        {
        } 


        public void Dispose()
        {
        //    _unmanagedWriteBuffer.Dispose();
            if (_compressionBuffer != null)
                _context.ReturnMemory(_compressionBuffer);

            _compressionBuffer = null;

            if (_innerBuffer != null)
                _context.ReturnMemory(_innerBuffer);
            _innerBuffer = null;
        }
    }
}
