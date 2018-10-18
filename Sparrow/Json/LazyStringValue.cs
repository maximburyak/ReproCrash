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
    public sealed unsafe class LazyStringValue :IDisposable
    {        
        private string _string;
        private byte* _buffer;        
        public byte* Buffer => _buffer;
        private int _size;
        public int Size => _size;            
        public AllocatedMemoryData AllocatedMemoryData;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue(string str, byte* buffer, int size)
        {            
            _size = size;
            _buffer = buffer;
            _string = str;        
        }   

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(LazyStringValue self)
        {
            if (self == null)
                return null;
            return self._string ??
                (self._string = Encoding.UTF8.GetString(self._buffer, self._size));
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
            IsDisposed = true;
        }      
    }
}
