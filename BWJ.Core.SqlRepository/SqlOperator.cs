using System;

namespace BWJ.Core.SqlRepository
{
    public enum SqlOperator
    {
        Equal,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        NotEqual,
        In,
        Like,
    }

    internal static class SqlOperatorExtensions
    {
        public static string ToOperator(this SqlOperator op)
        {
            switch (op)
            {
                case SqlOperator.Equal:
                    return "=";
                case SqlOperator.LessThan:
                    return "<";
                case SqlOperator.LessThanOrEqual:
                    return "<=";
                case SqlOperator.GreaterThan:
                    return ">";
                case SqlOperator.GreaterThanOrEqual:
                    return ">=";
                case SqlOperator.NotEqual:
                    return "<>";
                case SqlOperator.In:
                    return "IN";
                case SqlOperator.Like:
                    return "LIKE";
                default:
                    throw new NotSupportedException(nameof(op));
            }
        }
    }
}
