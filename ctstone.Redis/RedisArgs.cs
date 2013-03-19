using System;
using System.Collections.Generic;

namespace ctstone.Redis
{
    static class RedisArgs
    {
        /// <summary>
        /// Represents UNIX Epoch (Jan 1, 1970 00:00:00 UTC)
        /// </summary>
        public static DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Join arrays
        /// </summary>
        /// <param name="arrays">Arrays to join</param>
        /// <returns>Array of ToString() elements in each array</returns>
        public static string[] Concat(params object[][] arrays)
        {
            int count = 0;
            foreach (var ar in arrays)
                count += ar.Length;

            int pos = 0;
            string[] output = new string[count];
            for (int i = 0; i < arrays.Length; i++)
            {
                for (int j = 0; j < arrays[i].Length; j++)
                {
                    output[pos++] = arrays[i][j].ToString();
                }
            }
            return output;
        }

        /// <summary>
        /// Joine string with arrays
        /// </summary>
        /// <param name="str">Leading string element</param>
        /// <param name="arrays">Array to join</param>
        /// <returns>Array of str and ToString() elements of arrays</returns>
        public static string[] Concat(string str, params object[] arrays)
        {
            return Concat(new[] { str }, arrays);
        }

        /// <summary>
        /// Convert array of two-element tuple into flat array arguments
        /// </summary>
        /// <typeparam name="TItem1">Type of first item</typeparam>
        /// <typeparam name="TItem2">Type of second item</typeparam>
        /// <param name="tuples">Array of tuple arguments</param>
        /// <returns>Flattened array of arguments</returns>
        public static object[] GetTupleArgs<TItem1, TItem2>(Tuple<TItem1, TItem2>[] tuples)
        {
            List<object> args = new List<object>();
            foreach (var kvp in tuples)
                args.AddRange(new object[] { kvp.Item1, kvp.Item2 });

            return args.ToArray();
        }

        /// <summary>
        /// Parse score for +/- infinity and inclusive/exclusive
        /// </summary>
        /// <param name="score">Numeric base score</param>
        /// <param name="isExclusive">Score is exclusive, rather than inclusive</param>
        /// <returns>String representing Redis score/range notation</returns>
        public static string GetScore(double score, bool isExclusive)
        {
            if (score == Double.MinValue)
                return "-inf";
            else if (score == Double.MaxValue)
                return "+inf";
            else if (isExclusive)
                return '(' + score.ToString();
            else
                return score.ToString();
        }
    }
}
