using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;

namespace ctstone.Redis
{
    class HashMapper
    {
        public static Dictionary<string, string> GetDict(object[] fieldValues)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            if (fieldValues.Length == 0)
                return dict;
            string field, value;
            int i;
            for (i = 0; i < fieldValues.Length; i += 2)
            {
                field = fieldValues[i].ToString();
                value = fieldValues[i + 1].ToString();
                dict[field] = value;
            }
            return dict;
        }

        public static T ReflectHash<T>(string[] fields, string[] values)
            where T : new()
        {
            List<string> hash = new List<string>();
            for (int i = 0; i < fields.Length; i++)
            {
                hash.AddRange(new[] { fields[i], values[i] });
            }
            return ReflectHash<T>(hash.ToArray());
        }

        public static T ReflectHash<T>(object[] fieldValues)
            where T : new()
        {
            T obj = Activator.CreateInstance<T>();
            if (fieldValues.Length == 0)
                return obj;
            TypeConverter conv;
            PropertyInfo prop;
            string field, value;
            for (int i = 0; i < fieldValues.Length; i += 2)
            {
                field = fieldValues[i].ToString().Replace("-", String.Empty);
                value = fieldValues[i + 1].ToString();
                prop = typeof(T).GetProperty(field, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                if (prop == null || !prop.CanWrite)
                    continue;
                conv = TypeDescriptor.GetConverter(prop.PropertyType);
                if (!conv.CanConvertFrom(typeof(String)))
                    continue;
                if (prop.PropertyType == typeof(Boolean) && value == "1")
                    value = "true";
                prop.SetValue(obj, conv.ConvertFrom(value), null);
            }
            return obj;
        }
    }
}
