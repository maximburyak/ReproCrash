using System;

namespace Sparrow.Json
{
    //note: There are overlapping bits in the values,
    // so for some values HasFlag() can return invalid results.
    // This is by design, so bit packing can be done
    [Flags]
    public enum BlittableJsonToken : byte
    {        
        Integer = 3,     
        String = 5,        
        Null = 8        
    }
}
