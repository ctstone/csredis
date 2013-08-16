using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;

namespace ctstone.Redis
{
    static class HashMapper
    {
        public static Dictionary<string, string> ToDict(object[] fieldValues)
        {
            var dict = new Dictionary<string, string>();
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

        public static object[] FromDict(Dictionary<string, string> dict)
        {
            var ar = new List<object>();
            foreach (var keyValue in dict)
            {
                if (keyValue.Key != null && keyValue.Value != null)
                    ar.AddRange(new[] { keyValue.Key, keyValue.Value });
            }
            return ar.ToArray();
        }

        public static T ToObject<T>(object[] fieldValues)
            where T : class
        {
            /*T obj = Activator.CreateInstance<T>();
            
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
            return obj;*/
            if (fieldValues.Length == 0)
                return default(T);
            var dict = new Dictionary<string, string>();
            for (int i = 0; i < fieldValues.Length; i += 2)
            {
                string key = fieldValues[i].ToString().Replace("-", String.Empty);
                string value = fieldValues[i + 1].ToString();
                dict[key] = value;
            }
            return Serializer<T>.Deserialize(dict);
        }

        public static object[] FromObject<T>(T obj)
            where T : class
        {
            var dict = Serializer<T>.Serialize(obj);
            object[] ar = new object[dict.Count * 2];
            int i = 0;
            foreach (var item in dict)
            {
                ar[i++] = item.Key;
                ar[i++] = item.Value;
            }
            return ar;
        }
    }
}
