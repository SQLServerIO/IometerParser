//IOMeterParser thrown together by Wes Brown
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, and to permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
//
//using NDesk.Options for the command line parsing way tired of writing command line parsers!
//
//

using System;
using System.Data.SqlTypes;

namespace IometerParser
{
    public static class AccessSpecificationHeader
    {
        public static string AccessSpecificationName { get; set; }
    }

    public static class TestHeader
    {
        public static SqlGuid TestId { get; set; }
        public static Int32 TestType { get; set; }
        public static string TestDescription { get; set; }
        public static string Version { get; set; }
        public static DateTime TimeStamp { get; set; }
    }

    public static class GlobalVariables
    {
        public static string ComputerName { get; set; } //name of machine the test was executed on
        public static DateTime TestDate { get; set; } //time from file create
        public static string SQLServer { get; set; } //target server
        public static string SQLServerUser { get; set; } //sql user
        public static string SQLServerPassword { get; set; } //target server
        public static string Database { get; set; } //target database
        public static string TableName { get; set; } //table you want to insert into
        public static string IOMeterCSVFileName { get; set; } //File to import
        public static string IOMeterCSVDirectoryName { get; set; } //directory to import
        public static string CSVOutFile { get; set; } //File to export
    }
}