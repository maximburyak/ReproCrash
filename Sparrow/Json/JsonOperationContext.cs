using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {        
        private readonly JsonParserState _jsonParserState;
        private readonly BlittableJsonDocumentBuilder _documentBuilder;        
        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext();
        }

        public JsonOperationContext()
        {            
            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
                Reset(true);

                _documentBuilder.Dispose();                
            });                        
            
            _jsonParserState = new JsonParserState();
            _documentBuilder = new BlittableJsonDocumentBuilder(this, _jsonParserState, null);            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
            var allocatedMemory = new AllocatedMemoryData(requestedSize);
            return allocatedMemory;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetLongLivedMemory(int requestedSize)
        {
            var allocatedMemory = new AllocatedMemoryData(requestedSize);                        
            return allocatedMemory;
        }    
        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;
        private bool Disposed => _disposeOnceRunner.Disposed;
        public  void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            EnsureNotDisposed();        
            var state = new JsonParserState();
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(field.Length);

            int escapePositionsSize = JsonParserState.EscapePositionItemSize;

            int memorySize = maxByteCount + escapePositionsSize;
            var memory = GetLongLivedMemory(memorySize);

            fixed (char* pField = field)
            {
                var address = memory.Address;
                var actualSize = Encoding.UTF8.GetBytes(pField, field.Length, address, memory.SizeInBytes);                

                state.WriteEscapePositionsTo(address + actualSize);
                LazyStringValue result = new LazyStringValue(field, address, actualSize);
                result.AllocatedMemoryData = memory;
        
                return result;
            }
        }
        private void EnsureNotDisposed()
        {
            if (Disposed)
                ThrowObjectDisposed();
        }
        private void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException(nameof(JsonOperationContext));
        }

        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            _documentBuilder.Reset();            
        }        
    }
}
