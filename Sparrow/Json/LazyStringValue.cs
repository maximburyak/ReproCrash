using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Json
{
    // PERF: Sealed because in CoreCLR 2.0 it will devirtualize virtual calls methods like GetHashCode.
    public sealed unsafe class LazyStringValue :IEquatable<string>,
         IEquatable<LazyStringValue>, IDisposable
    {
        internal JsonOperationContext __context;
        internal JsonOperationContext _context
        {
            get
            {

                Console.WriteLine(Environment.StackTrace);
                return __context;
            }
            set
            {

                __context = value;
            }
        }

        private string _string;

        private byte* _buffer;
        public byte* Buffer => _buffer;
        private int _size;
        public int Size => _size;

        private int _length;


        [ThreadStatic]
        private static byte[] _lazyStringTempComparisonBuffer;

        public int[] EscapePositions;
        public AllocatedMemoryData AllocatedMemoryData;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue(string str, byte* buffer, int size, JsonOperationContext context)
        {
            Debug.Assert(context != null);
            _context = context;
            _size = size;
            _buffer = buffer;
            _string = str;
            _length = -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(string other)
        {

            if (_string != null)
                return string.Equals(_string, other, StringComparison.Ordinal);

            var sizeInBytes = Encoding.UTF8.GetMaxByteCount(other.Length);

            if (_lazyStringTempComparisonBuffer == null || _lazyStringTempComparisonBuffer.Length < other.Length)
                _lazyStringTempComparisonBuffer = new byte[(sizeInBytes)];

            fixed (char* pOther = other)
            fixed (byte* pBuffer = _lazyStringTempComparisonBuffer)
            {
                var tmpSize = Encoding.UTF8.GetBytes(pOther, other.Length, pBuffer, sizeInBytes);
                if (Size != tmpSize)
                    return false;

                return Memory.CompareInline(Buffer, pBuffer, tmpSize) == 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue other)
        {

            int size = Size;
            if (other.Size != size)
                return false;

            return Memory.CompareInline(Buffer, other.Buffer, size) == 0;
        }

        public int CompareTo(string other)
        {
            if (_string != null)
                return string.Compare(_string, other, StringComparison.Ordinal);

            var sizeInBytes = Encoding.UTF8.GetMaxByteCount(other.Length);

            if (_lazyStringTempComparisonBuffer == null || _lazyStringTempComparisonBuffer.Length < other.Length)
                _lazyStringTempComparisonBuffer = new byte[(sizeInBytes)];

            fixed (char* pOther = other)
            fixed (byte* pBuffer = _lazyStringTempComparisonBuffer)
            {
                var tmpSize = Encoding.UTF8.GetBytes(pOther, other.Length, pBuffer, sizeInBytes);
                return Compare(pBuffer, tmpSize);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareTo(LazyStringValue other)
        {
            if (other.Buffer == Buffer && other.Size == Size)
                return 0;
            return Compare(other.Buffer, other.Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(byte* other, int otherSize)
        {
            int size = Size;
            var result = Memory.CompareInline(Buffer, other, Math.Min(size, otherSize));
            return result == 0 ? size - otherSize : result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(LazyStringValue self)
        {
            if (self == null)
                return null;

            return self._string ??
                (self._string = Encoding.UTF8.GetString(self._buffer, self._size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator byte[] (LazyStringValue self)
        {
            var valueAsString = (string)self;
            return Convert.FromBase64String(valueAsString);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            var s = obj as string;
            if (s != null)
                return Equals(s);
            var comparer = obj as LazyStringValue;
            if (comparer != null)
                return Equals(comparer);

            return ReferenceEquals(obj, this);
        }

        public unsafe override int GetHashCode()
        {

            return Encoding.UTF8.GetString(Buffer, Size).GetHashCode();
        }

        public override string ToString()
        {
            return (string)this; // invoke the implicit string conversion
        }

        public bool IsDisposed;

        private void ThrowAlreadyDisposed()
        {
            throw new ObjectDisposedException(nameof(LazyStringValue));
        }

        public void Dispose()
        {
            if (IsDisposed)
                ThrowAlreadyDisposed();

            if (AllocatedMemoryData != null)
            {
                _context.ReturnMemory(AllocatedMemoryData);
            }
            IsDisposed = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(string str, byte* buffer, int size)
        {
            Debug.Assert(size >= 0);
            _size = size;
            _buffer = buffer;
            _string = str;
            _length = -1;
            EscapePositions = null;
            IsDisposed = false;
            AllocatedMemoryData = null;
        }
    }
}
