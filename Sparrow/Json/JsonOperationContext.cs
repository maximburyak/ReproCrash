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
using Sparrow.Global;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

#if VALIDATE
using Sparrow.Platform;
#endif

namespace Sparrow.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public class JsonOperationContext : IDisposable
    {
        private int _generation;
        public const int InitialStreamSize = 4096;
        private const int MaxInitialStreamSize = 16 * 1024 * 1024;
        private readonly int _initialSize;
        private readonly int _longLivedSize;
        private readonly ArenaMemoryAllocator _arenaAllocator;
        private ArenaMemoryAllocator _arenaAllocatorForLongLivedValues;
        private AllocatedMemoryData _tempBuffer;
        private List<string> _normalNumbersStringBuffers = new List<string>(5);
        private string _hugeNumbersBuffer;

        private readonly Dictionary<string, LazyStringValue> _fieldNames = new Dictionary<string, LazyStringValue>();

        private struct PathCacheHolder
        {
            public PathCacheHolder(Dictionary<StringSegment, object> path, Dictionary<int, object> byIndex)
            {
                Path = path;
                ByIndex = byIndex;
            }

            public readonly Dictionary<StringSegment, object> Path;
            public readonly Dictionary<int, object> ByIndex;
        }

        private int _numberOfAllocatedPathCaches = -1;
        private readonly PathCacheHolder[] _allocatePathCaches = new PathCacheHolder[512];
        private Stack<MemoryStream> _cachedMemoryStreams = new Stack<MemoryStream>();

        private int _numberOfAllocatedStringsValues;
        private readonly List<LazyStringValue> _allocateStringValues = new List<LazyStringValue>(256);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquirePathCache(out Dictionary<StringSegment, object> pathCache, out Dictionary<int, object> pathCacheByIndex)
        {
            // PERF: Avoids allocating gigabytes in FastDictionary instances on high traffic RW operations like indexing. 
            if (_numberOfAllocatedPathCaches >= 0)
            {
                var cache = _allocatePathCaches[_numberOfAllocatedPathCaches--];
                Debug.Assert(cache.Path != null);
                Debug.Assert(cache.ByIndex != null);

                pathCache = cache.Path;
                pathCacheByIndex = cache.ByIndex;

                return;
            }

            pathCache = new Dictionary<StringSegment, object>();
            pathCacheByIndex = new Dictionary<int, object>();
        }

        public void ReleasePathCache(Dictionary<StringSegment, object> pathCache, Dictionary<int, object> pathCacheByIndex)
        {
            if (_numberOfAllocatedPathCaches < _allocatePathCaches.Length - 1 && pathCache.Count < 256)
            {
                pathCache.Clear();
                pathCacheByIndex.Clear();

                _allocatePathCaches[++_numberOfAllocatedPathCaches] = new PathCacheHolder(pathCache, pathCacheByIndex);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LazyStringValue AllocateStringValue(string str, byte* ptr, int size)
        {
            if (_numberOfAllocatedStringsValues < _allocateStringValues.Count)
            {
                var lazyStringValue = _allocateStringValues[_numberOfAllocatedStringsValues++];
                lazyStringValue.Renew(str, ptr, size);
                return lazyStringValue;
            }

            var allocateStringValue = new LazyStringValue(str, ptr, size, this);
            if (_numberOfAllocatedStringsValues < 25 * 1000)
            {
                _allocateStringValues.Add(allocateStringValue);
                _numberOfAllocatedStringsValues++;
            }
            return allocateStringValue;
        }
                   
        public const int LargeBufferSize = 128 * Constants.Size.Kilobyte;
        
       
        internal unsafe class BufferSegment
        {
            public byte[] Array;
            public int Offset;
            public int Count;
            public byte* Ptr;
        }

        
        public unsafe class ManagedPinnedBuffer : IDisposable
        {
            public const int Size =  32 * Constants.Size.Kilobyte;

            internal BufferSegment BufferInstance;
            public ArraySegment<byte> Buffer;
            public int Length;
            public int Valid, Used;
            public byte* Pointer;

            private bool _disposed;
            public GCHandle Handle;

            public void Dispose()
            {
                if (_disposed)
                    return;
                _disposed = true;
                GC.SuppressFinalize(this);
                var bufferBefore = BufferInstance;
                BufferInstance = null;
                Buffer = new ArraySegment<byte>();

                Length = 0;
                Valid = Used = 0;
                Pointer = null;

            }

            ~ManagedPinnedBuffer()
            {
                if (Handle.IsAllocated)
                    Handle.Free();
            }


            public ManagedPinnedBuffer(JsonOperationContext ctx)
            {
                GC.SuppressFinalize(this); // we only want finalization if we have values                
            }

        }      
       

        private Stack<ManagedPinnedBuffer> _managedBuffers;

        public CachedProperties CachedProperties;

        private readonly JsonParserState _jsonParserState;
        private readonly BlittableJsonDocumentBuilder _documentBuilder;

        public int Generation => _generation;

        public long AllocatedMemory => _arenaAllocator.TotalUsed;

        protected readonly SharedMultipleUseFlag LowMemoryFlag;

        public static JsonOperationContext ShortTermSingleUse()
        {
            return new JsonOperationContext(4096, 1024, SharedMultipleUseFlag.None);
        }

        public JsonOperationContext(int initialSize, int longLivedSize, SharedMultipleUseFlag lowMemoryFlag)
        {
            Debug.Assert(lowMemoryFlag != null);
            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
#if MEM_GUARD_STACK
                ElectricFencedMemory.DecrementConext();
                ElectricFencedMemory.UnRegisterContextAllocation(this);
#endif

                Reset(true);

                _documentBuilder.Dispose();
                _arenaAllocator.Dispose();
                _arenaAllocatorForLongLivedValues?.Dispose();

                if (_managedBuffers != null)
                {
                    foreach (var managedPinnedBuffer in _managedBuffers)
                    {
                        if (managedPinnedBuffer is IDisposable s)
                            s.Dispose();
                    }

                    _managedBuffers = null;
                }
            });

            _initialSize = initialSize;
            _longLivedSize = longLivedSize;
            _arenaAllocator = new ArenaMemoryAllocator(lowMemoryFlag, initialSize);
            _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(lowMemoryFlag, longLivedSize);
            CachedProperties = new CachedProperties(this);
            _jsonParserState = new JsonParserState();
            _documentBuilder = new BlittableJsonDocumentBuilder(this, _jsonParserState, null);
            LowMemoryFlag = lowMemoryFlag;

#if MEM_GUARD_STACK
            ElectricFencedMemory.IncrementConext();
            ElectricFencedMemory.RegisterContextAllocation(this,Environment.StackTrace);
#endif
        }

        public ReturnBuffer GetManagedBuffer(out ManagedPinnedBuffer buffer)
        {
            EnsureNotDisposed();
            buffer= new ManagedPinnedBuffer(this);
            buffer.Buffer = new ArraySegment<byte>(new byte[1024*64]);
            buffer.Handle = GCHandle.Alloc(buffer.Buffer.Array, GCHandleType.Pinned);
            buffer.Valid = buffer.Used = 0;
            return new ReturnBuffer(buffer, this);
        }

        public struct ReturnBuffer : IDisposable
        {
            private  ManagedPinnedBuffer _buffer;
            private readonly JsonOperationContext _parent;

            public ReturnBuffer(ManagedPinnedBuffer buffer, JsonOperationContext parent)
            {
                _buffer = buffer;
                _parent = parent;
            }

            public void Dispose()
            {
                if (_buffer == null)
                    return;

                //_parent disposal sets _managedBuffers to null,
                //throwing ObjectDisposedException() to make it more visible
                if (_parent.Disposed)
                    ThrowParentWasDisposed();

                _parent._managedBuffers.Push(_buffer);
                _buffer = null;
            }

            private void ThrowParentWasDisposed()
            {
                throw new ObjectDisposedException(
                    "ReturnBuffer should not be disposed after it's parent operation context was disposed");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetMemory(int requestedSize)
        {
#if DEBUG || VALIDATE
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));
#endif

            var allocatedMemory = _arenaAllocator.Allocate(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
#if DEBUG
            allocatedMemory.IsLongLived = false;
#endif
            return allocatedMemory;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData GetLongLivedMemory(int requestedSize)
        {
#if DEBUG || VALIDATE
            if (requestedSize <= 0)
                throw new ArgumentException(nameof(requestedSize));
#endif
            //we should use JsonOperationContext in single thread
            if (_arenaAllocatorForLongLivedValues == null)
            {
                //_arenaAllocatorForLongLivedValues == null when the context is after Reset() but before Renew()
                ThrowAlreadyDisposedForLongLivedAllocator();

                //make compiler happy, previous row will throw
                return null;
            }

            var allocatedMemory = _arenaAllocatorForLongLivedValues.Allocate(requestedSize);
            allocatedMemory.ContextGeneration = Generation;
            allocatedMemory.Parent = this;
#if DEBUG
            allocatedMemory.IsLongLived = true;
#endif
            return allocatedMemory;
        }

        private void ThrowAlreadyDisposedForLongLivedAllocator()
        {
            throw new ObjectDisposedException("Could not allocated long lived memory, because the context is after Reset() but before Renew(). Is it possible that you have tried to use the context AFTER it was returned to the context pool?");
        }

    
        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;
        private bool Disposed => _disposeOnceRunner.Disposed;
        public  void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue GetLazyStringForFieldWithCaching(StringSegment key)
        {
            EnsureNotDisposed();

            var field = key.Value; // This will allocate if we are using a substring. 
            if (_fieldNames.TryGetValue(field, out LazyStringValue value))
            {
                //sanity check, in case the 'value' is manually disposed outside of this function
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue GetLazyStringForFieldWithCaching(string field)
        {
            EnsureNotDisposed();
            if (_fieldNames.TryGetValue(field, out LazyStringValue value))
            {
                // PERF: This is usually the most common scenario, so actually being contiguous improves the behavior.
                Debug.Assert(value.IsDisposed == false);
                return value;
            }

            return GetLazyStringForFieldWithCachingUnlikely(field);
        }

        private LazyStringValue GetLazyStringForFieldWithCachingUnlikely(StringSegment key)
        {
            EnsureNotDisposed();
            LazyStringValue value = GetLazyString(key, longLived: true);
            _fieldNames[key] = value;

            //sanity check, in case the 'value' is manually disposed outside of this function
            Debug.Assert(value.IsDisposed == false);
            return value;
        }

        public LazyStringValue GetLazyString(string field)
        {
            EnsureNotDisposed();

            if (field == null)
                return null;

            return GetLazyString(field, longLived: false);
        }

        
        private unsafe LazyStringValue GetLazyString(StringSegment field, bool longLived)
        {
            var state = new JsonParserState();
            var maxByteCount = Encoding.UTF8.GetMaxByteCount(field.Length);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(field);

            int memorySize = maxByteCount + escapePositionsSize;
            var memory = longLived ? GetLongLivedMemory(memorySize) : GetMemory(memorySize);

            fixed (char* pField = field.Buffer)
            {
                var address = memory.Address;
                var actualSize = Encoding.UTF8.GetBytes(pField + field.Offset, field.Length, address, memory.SizeInBytes);

                state.FindEscapePositionsIn(address, actualSize, escapePositionsSize);

                state.WriteEscapePositionsTo(address + actualSize);
                LazyStringValue result = longLived == false ? AllocateStringValue(field, address, actualSize) : new LazyStringValue(field, address, actualSize, this);
                result.AllocatedMemoryData = memory;

                if (state.EscapePositions.Count > 0)
                {
                    Console.WriteLine( "Has escape positions!");
                    result.EscapePositions = state.EscapePositions.ToArray();
                }
                return result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LazyStringValue GetLazyStringValue(byte* ptr)
        {
            // See format of the lazy string ID in the GetLowerIdSliceAndStorageKey method
            var size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out var offset);
            return AllocateStringValue(null, ptr + offset, size);
        }

        public BlittableJsonReaderObject ReadForDisk(Stream stream, string documentId)
        {
            return ParseToMemory(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public ValueTask<BlittableJsonReaderObject> ReadForDiskAsync(Stream stream, string documentId, CancellationToken? token = null)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk, token);
        }

        public ValueTask<BlittableJsonReaderObject> ReadForMemoryAsync(Stream stream, string documentId, CancellationToken? token = null)
        {
            return ParseToMemoryAsync(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None, token);
        }

        public BlittableJsonReaderObject ReadForMemory(Stream stream, string documentId)
        {
            return ParseToMemory(stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public unsafe BlittableJsonReaderObject ReadForMemory(string jsonString, string documentId)
        {
            // todo: maybe use ManagedPinnedBuffer here
            var maxByteSize = Encoding.UTF8.GetMaxByteCount(jsonString.Length);

            fixed (char* val = jsonString)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(maxByteSize);
                try
                {
                    fixed (byte* buf = buffer)
                    {
                        Encoding.UTF8.GetBytes(val, jsonString.Length, buf, maxByteSize);
                        using (var ms = new MemoryStream(buffer))
                        {
                            return ReadForMemory(ms, documentId);
                        }
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        public BlittableJsonReaderObject Read(Stream stream, string documentId)
        {
            var state = BlittableJsonDocumentBuilder.UsageMode.ToDisk;
            return ParseToMemory(stream, documentId, state);
        }

        private BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode)
        {
            ManagedPinnedBuffer bytes;
            using (GetManagedBuffer(out bytes))
            {
                return ParseToMemory(stream, debugTag, mode, bytes);
            }
        }

        public BlittableJsonReaderObject ParseToMemory(Stream stream, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode,
            ManagedPinnedBuffer bytes)
        {

            EnsureNotDisposed();

            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = stream.Read(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length);
                        EnsureNotDisposed();
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        public unsafe BlittableJsonReaderObject ParseBuffer(byte* buffer, int length, string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode)
        {

            EnsureNotDisposed();

            _jsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(this, _jsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(this, mode, debugTag, parser, _jsonParserState))
            {
                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                parser.SetBuffer(buffer, length);

                if (builder.Read() == false)
                    throw new EndOfStreamException("Buffer ended without reaching end of json content");

                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }

        private void EnsureNotDisposed()
        {
            if (Disposed)
                ThrowObjectDisposed();
        }

        private ValueTask<BlittableJsonReaderObject> ParseToMemoryAsync(Stream stream, string documentId, BlittableJsonDocumentBuilder.UsageMode mode, CancellationToken? token = null)
        {
            using (GetManagedBuffer(out ManagedPinnedBuffer bytes))
                return ParseToMemoryAsync(stream, documentId, mode, bytes, token);
        }

        public async ValueTask<BlittableJsonReaderObject> ParseToMemoryAsync(Stream stream, string documentId, BlittableJsonDocumentBuilder.UsageMode mode, ManagedPinnedBuffer bytes,
            CancellationToken? token = null,
            int maxSize = int.MaxValue)
        {
            EnsureNotDisposed();

            _jsonParserState.Reset();
            UnmanagedJsonParser parser = null;
            BlittableJsonDocumentBuilder builder = null;
            var generation = _generation;
            var streamDisposer = token?.Register(stream.Dispose);
            try
            {
                parser = new UnmanagedJsonParser(this, _jsonParserState, documentId);
                builder = new BlittableJsonDocumentBuilder(this, mode, documentId, parser, _jsonParserState);

                CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    token?.ThrowIfCancellationRequested();
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = token.HasValue
                            ? await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length, token.Value).ConfigureAwait(false)
                            : await stream.ReadAsync(bytes.Buffer.Array, bytes.Buffer.Offset, bytes.Length).ConfigureAwait(false);

                        EnsureNotDisposed();

                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                        maxSize -= read;
                        if (maxSize < 0)
                            throw new ArgumentException($"The maximum size allowed for {documentId} ({maxSize}) has been exceeded, aborting");
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
            finally
            {
                streamDisposer?.Dispose();
                DisposeIfNeeded(generation, parser, builder);
            }
        }

        private void DisposeIfNeeded(int generation, UnmanagedJsonParser parser, BlittableJsonDocumentBuilder builder)
        {
            // if the generation has changed, that means that we had reset the context
            // this can happen if we were waiting on an async call for a while, got timed out / error / something
            // and the context was reset before we got back from the async call
            // since the full context was reset, there is no point in trying to dispose things, they were already 
            // taken care of
            if (generation == _generation)
            {
                parser?.Dispose();
                builder?.Dispose();
            }
        }

        private void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException(nameof(JsonOperationContext));
        }

        protected internal virtual void Renew()
        {
            _arenaAllocator.RenewArena();
            if (_arenaAllocatorForLongLivedValues == null)
            {
                _arenaAllocatorForLongLivedValues = new ArenaMemoryAllocator(LowMemoryFlag, _longLivedSize);
                CachedProperties = new CachedProperties(this);
            }
        }

        protected internal virtual unsafe void Reset(bool forceReleaseLongLivedAllocator = false)
        {
            if (_tempBuffer != null && _tempBuffer.Address != null)
            {
                _arenaAllocator.Return(_tempBuffer);
                _tempBuffer = null;
            }

            _documentBuilder.Reset();

            // We don't reset _arenaAllocatorForLongLivedValues. It's used as a cache buffer for long lived strings like field names.
            // When a context is re-used, the buffer containing those field names was not reset and the strings are still valid and alive.

            var allocatorForLongLivedValues = _arenaAllocatorForLongLivedValues;
            if (allocatorForLongLivedValues != null &&
                (allocatorForLongLivedValues.Allocated > _initialSize || forceReleaseLongLivedAllocator))
            {
                foreach (var mem in _fieldNames.Values)
                {
                    _arenaAllocatorForLongLivedValues.Return(mem.AllocatedMemoryData);
                    mem.AllocatedMemoryData = null;
                    mem.Dispose();
                }

                _arenaAllocatorForLongLivedValues = null;
                // at this point, the long lived section is far too large, this is something that can happen
                // if we have dynamic properties. A back of the envelope calculation gives us roughly 32K 
                // property names before this kicks in, which is a true abuse of the system. In this case, 
                // in order to avoid unlimited growth, we'll reset the long lived section
                allocatorForLongLivedValues.Dispose();

                _fieldNames.Clear();
                CachedProperties = null; // need to release this so can be collected
            }
            _arenaAllocator.ResetArena();
            _numberOfAllocatedStringsValues = 0;
            _generation = _generation + 1;

            if (_pooledArrays != null )
            {
                foreach (var pooledTypesKVP in _pooledArrays)
                {
                    foreach (var pooledArraysOfCurrentType in pooledTypesKVP.Value.Array)
                    {
                        pooledTypesKVP.Value.Releaser(pooledArraysOfCurrentType);
                    }
                }

                _pooledArrays = null;
            }
        }

        public void Write(Stream stream, BlittableJsonReaderObject json)
        {
            EnsureNotDisposed();
           
        }


        public unsafe double ParseDouble(byte* ptr, int length)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return double.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseDouble(byte* ptr, int length, out double val)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);
                        
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return double.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture,out val);            
        }

        public unsafe decimal ParseDecimal(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return decimal.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseDecimal(byte* ptr, int length, out decimal val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return decimal.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe float ParseFloat(byte* ptr, int length)
        {
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return float.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseFloat(byte* ptr, int length, out float val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return float.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe long ParseLong(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return long.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseLong(byte* ptr, int length, out long val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return long.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }

        public unsafe ulong ParseULong(byte* ptr, int length)
        {
            EnsureNotDisposed();
            var stringBuffer= InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return ulong.Parse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
        }

        public unsafe bool TryParseULong(byte* ptr, int length, out ulong val)
        {
            EnsureNotDisposed();
            var stringBuffer = InitializeStringBufferForNumberParsing(ptr, length);

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return ulong.TryParse(stringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture, out val);
        }       

        private unsafe string InitializeStringBufferForNumberParsing(byte* ptr, int length)
        {
            var lengthsNextPowerOf2 = (length);

            var actualPowerOf2 = (int)Math.Pow(lengthsNextPowerOf2, 0.5);
            string stringBuffer;
            if (actualPowerOf2 <= _normalNumbersStringBuffers.Count)
            {
                stringBuffer = _normalNumbersStringBuffers[actualPowerOf2 - 1];

                if (stringBuffer == null)
                {
                    stringBuffer = _normalNumbersStringBuffers[actualPowerOf2 - 1] = new string(' ', lengthsNextPowerOf2);
                }
            }
            else
            {
                stringBuffer = _hugeNumbersBuffer;
                if (_hugeNumbersBuffer == null || length > _hugeNumbersBuffer.Length)
                    stringBuffer = _hugeNumbersBuffer = new string(' ', length);
            }
            // we should support any length of LazyNumber, therefore, we do not validate it's length
            
            
            // here we assume a clear char <- -> byte conversion, we only support
            // utf8, and those cleanly transfer
            fixed (char* pChars = stringBuffer)
            {
                int i = 0;

                for (; i < length; i++)
                {
                    pChars[i] = (char)ptr[i];
                }
                for (; i < stringBuffer.Length; i++)
                {
                    pChars[i] = ' ';
                }
            }

            return stringBuffer;
        }

        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            EnsureNotDisposed();
            return _arenaAllocator.GrowAllocation(allocation, sizeIncrease);
        }

        public MemoryStream CheckoutMemoryStream()
        {
            EnsureNotDisposed();
            if (_cachedMemoryStreams.Count == 0)
            {
                return new MemoryStream();
            }

            return _cachedMemoryStreams.Pop();
        }

        public void ReturnMemoryStream(MemoryStream stream)
        {
            EnsureNotDisposed();
            stream.SetLength(0);
            _cachedMemoryStreams.Push(stream);
        }

        public void ReturnMemory(AllocatedMemoryData allocation)
        {
            EnsureNotDisposed();
            if (_generation != allocation.ContextGeneration)
                ThrowUseAfterFree(allocation);

            _arenaAllocator.Return(allocation);
        }

        private void ThrowUseAfterFree(AllocatedMemoryData allocation)
        {
#if MEM_GUARD_STACK || TRACK_ALLOCATED_MEMORY_DATA
            throw new InvalidOperationException(
                $"UseAfterFree detected! Attempt to return memory from previous generation, Reset has already been called and the memory reused! Allocated by: {allocation.AllocatedBy}. Thread name: {Thread.CurrentThread.Name}");
#else
            throw new InvalidOperationException(
                $"UseAfterFree detected! Attempt to return memory from previous generation, Reset has already been called and the memory reused! Thread name: {Thread.CurrentThread.Name}");
#endif
        }

        public AvoidOverAllocationScope AvoidOverAllocation()
        {
            EnsureNotDisposed();
            _arenaAllocator.AvoidOverAllocation = true;
            return new AvoidOverAllocationScope(this);
        }

        public struct AvoidOverAllocationScope : IDisposable
        {
            private JsonOperationContext _parent;
            public AvoidOverAllocationScope(JsonOperationContext parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent._arenaAllocator.AvoidOverAllocation = false;
            }
        }

        private Dictionary<Type, (Action<Array> Releaser, List<Array> Array)> _pooledArrays = null;

        public T[] AllocatePooledArray<T>(int size)
        {
            if (_pooledArrays == null)
                _pooledArrays = new Dictionary<Type, (Action<Array> Releaser, List<Array> Array)>();
            
            

            if (_pooledArrays.TryGetValue(typeof(T), out var allocationsArray) == false)
            {
                void Releaser(Array x) => ArrayPool<T>.Shared.Return((T[])x, true);

                allocationsArray = (Releaser, new List<Array>());
                _pooledArrays[typeof(T)] = allocationsArray;
            }

            var allocatedArray = ArrayPool<T>.Shared.Rent(size);
            allocationsArray.Array.Add(allocatedArray);
            return allocatedArray;
        }
    }
}
