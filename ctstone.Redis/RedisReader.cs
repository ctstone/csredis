using System;
using System.IO;
using System.Text;

namespace ctstone.Redis
{
    static class RedisReader
    {
        public static RedisMessage ReadType(Stream stream)
        {
            RedisMessage type = (RedisMessage)stream.ReadByte();
            ActivityTracer.Verbose("Received response type: {0}", type);
            if (type == RedisMessage.Error)
                throw new RedisException(ReadStatus(stream, false));
            return type;
        }

        // redis type = STATUS
        public static string ReadStatus(Stream stream)
        {
            return ReadStatus(stream, true);
        }
        public static string ReadStatus(Stream stream, bool checkType)
        {
            if (checkType)
                ExpectType(stream, RedisMessage.Status);
            return ReadLine(stream);
        }

        // redis type == INT
        public static long ReadInt(Stream stream)
        {
            return ReadInt(stream, true);
        }
        public static long ReadInt(Stream stream, bool checkType)
        {
            if (checkType)
                ExpectType(stream, RedisMessage.Int);

            string line = ReadLine(stream);
            return Int64.Parse(line.ToString());
        }

        // redis type == BULK
        public static string ReadBulkUTF8(Stream stream)
        {
            return ReadBulkUTF8(stream, true);
        }
        public static string ReadBulkUTF8(Stream stream, bool checkType)
        {
            byte[] bulk = ReadBulk(stream, checkType);
            if (bulk == null)
                return null;
            return Encoding.UTF8.GetString(bulk);
        }
        public static byte[] ReadBulk(Stream stream)
        {
            return ReadBulk(stream, true);
        }
        public static byte[] ReadBulk(Stream stream, bool checkType)
        {
            if (checkType)
                ExpectType(stream, RedisMessage.Bulk);
            int size = (int)ReadInt(stream, false);
            if (size == -1)
                return null;

            byte[] bulk = new byte[size];
            int bytes_read = FillBuffer(stream, bulk);
            ExpectBytesRead(size, bytes_read);
            ReadCRLF(stream);
            return bulk;
        }

        

        public static void ReadBulk(Stream stream, Stream destination, int bufferSize, bool checkType)
        {
            if (checkType)
                ExpectType(stream, RedisMessage.Bulk);
            int size = (int)ReadInt(stream, false);
            if (size == -1)
                return;
            
            byte[] buffer = new byte[bufferSize];
            long bytes_read = 0;
            int bytes_buffered;
            while (bytes_read < size) 
            {
                bytes_read += bytes_buffered = FillBuffer(stream, buffer); 
                destination.Write(buffer, 0, bytes_buffered);
            }
            ExpectBytesRead(size, bytes_read);
            ReadCRLF(stream);
        }

        // redis type == MULTIBULK
        public static string[] ReadMultiBulkUTF8(Stream stream)
        {
            object[] result = ReadMultiBulk(stream);
            if (result == null)
                return null;
            string[] strings = new string[result.Length];
            for (int i = 0; i < result.Length; i++)
                strings[i] = result[i] == null ? null : result[i].ToString();
            return strings;
        }
        public static object[] ReadMultiBulk(Stream stream)
        {
            return ReadMultiBulk(stream, true);
        }
        public static object[] ReadMultiBulk(Stream stream, bool checkType)
        {
            if (checkType)
                ExpectType(stream, RedisMessage.MultiBulk);
            long count = ReadInt(stream, false);
            if (count == -1)
                return null;

            object[] lines = new object[count];
            for (int i = 0; i < count; i++)
                lines[i] = Read(stream);
            return lines;
        }


        // redis type == ANY
        public static object Read(Stream stream)
        {
            RedisMessage type = ReadType(stream);
            switch (type)
            {
                case RedisMessage.Bulk:
                    return ReadBulkUTF8(stream, false);

                case RedisMessage.Int:
                    return ReadInt(stream, false);

                case RedisMessage.MultiBulk:
                    return ReadMultiBulk(stream, false);

                case RedisMessage.Status:
                    return ReadStatus(stream, false);

                case RedisMessage.Error:
                    throw new RedisException(ReadStatus(stream, false));

                default:
                    throw new RedisProtocolException("Unsupported response type: " + type);
            }
        }

        public static void ExpectType(Stream stream, RedisMessage expectedType)
        {
            RedisMessage type = ReadType(stream);
            if (type != expectedType)
                throw new RedisProtocolException(String.Format("Unexpected response type: {0} (expecting {1})", type, expectedType));
        }

        private static string ReadLine(Stream stream)
        {
            StringBuilder sb = new StringBuilder();
            char c;
            bool should_break = false;
            while (true)
            {
                c = (char)stream.ReadByte();
                if (c == RedisConnection.EOL[0])
                    should_break = true;
                else if (c == RedisConnection.EOL[1] && should_break)
                    break;
                else
                {
                    sb.Append(c);
                    should_break = false;
                }
            }
            return sb.ToString();
        }

        public static void ReadCRLF(Stream stream)
        {
            var r = stream.ReadByte();
            var n = stream.ReadByte();
            if (r != (byte)13 && n != (byte)10)
                throw new RedisProtocolException(String.Format("Expecting CRLF; got bytes: {0}, {1}", r, n));
        }

        static int FillBuffer(Stream stream, byte[] buffer)
        {
            int bytes_read = 0;
            while (bytes_read < buffer.Length)
                bytes_read += stream.Read(buffer, bytes_read, buffer.Length - bytes_read);
            return bytes_read;
        }

        static void ExpectBytesRead(long expecting, long actual)
        {
            if (actual != expecting)
                throw new RedisProtocolException(String.Format("Expecting {0} bytes; got {1} bytes", expecting, actual));
        }
    }
}
