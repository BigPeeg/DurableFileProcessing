using System;
using System.Collections.Generic;
using System.Text;

namespace DurableFileProcessing
{
    public enum ProcessingOutcome
    {
        New, Rebuilt, Unmanaged, Failed, Error 
    }
}
