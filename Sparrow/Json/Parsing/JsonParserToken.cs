namespace Sparrow.Json.Parsing
{
    public enum JsonParserToken
    {        
        Null            = 1 << 1,
        False           = 1 << 2,
        True            = 1 << 3,
        String          = 1 << 4,
        Separator       = 1 << 7,
        StartObject     = 1 << 8,
        StartArray      = 1 << 9,
        EndArray        = 1 << 10,
        EndObject       = 1 << 11,
    }

    // should never be visible externally
    public enum JsonParserTokenContinuation
    {
        None                    =   0,
        PartialNull             =   1 << 26,
        PartialString           =   1 << 28,
        PartialPreamble         =   1 << 30,
    }
}