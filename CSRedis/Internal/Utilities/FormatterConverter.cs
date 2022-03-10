using System;
using System.Globalization;
using System.Runtime.Serialization;

namespace CSRedis.Internal.Utilities
{
	/// <summary>
	/// This is only necessary on .NET Core as it does not have all of
	/// System.Runtime.Serialization ported over.
	/// </summary>
	internal class FormatterConverter : IFormatterConverter
	{
		public FormatterConverter()
		{
		}

		public Object Convert(Object value, Type type)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
		}

		public Object Convert(Object value, TypeCode typeCode)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ChangeType(value, typeCode, CultureInfo.InvariantCulture);
		}

		public bool ToBoolean(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToBoolean(value, CultureInfo.InvariantCulture);
		}

		public char ToChar(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToChar(value, CultureInfo.InvariantCulture);
		}

		public sbyte ToSByte(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToSByte(value, CultureInfo.InvariantCulture);
		}

		public byte ToByte(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToByte(value, CultureInfo.InvariantCulture);
		}

		public short ToInt16(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToInt16(value, CultureInfo.InvariantCulture);
		}

		public ushort ToUInt16(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToUInt16(value, CultureInfo.InvariantCulture);
		}

		public int ToInt32(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToInt32(value, CultureInfo.InvariantCulture);
		}

		public uint ToUInt32(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToUInt32(value, CultureInfo.InvariantCulture);
		}

		public long ToInt64(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToInt64(value, CultureInfo.InvariantCulture);
		}

		public ulong ToUInt64(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToUInt64(value, CultureInfo.InvariantCulture);
		}

		public float ToSingle(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToSingle(value, CultureInfo.InvariantCulture);
		}

		public double ToDouble(Object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToDouble(value, CultureInfo.InvariantCulture);
		}

		public decimal ToDecimal(object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToDecimal(value, CultureInfo.InvariantCulture);
		}

		public DateTime ToDateTime(object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToDateTime(value, CultureInfo.InvariantCulture);
		}

		public string ToString(object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			return System.Convert.ToString(value, CultureInfo.InvariantCulture);
		}
	}
}