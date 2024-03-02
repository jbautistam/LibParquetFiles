namespace Bau.Libraries.LibParquetFiles.Readers;

/// <summary>
///		Colección de filtros
/// </summary>
public class ParquetFiltersCollection
{
	/// <summary>
	///		Añade un filtro
	/// </summary>
	public void Add(string column, ParquetFilter.ConditionType condition, object? value1, object? value2 = null)
	{
		Add(new ParquetFilter(column, condition, value1, value2));
	}

	/// <summary>
	///		Añade un filtro
	/// </summary>
	public void Add(ParquetFilter filter)
	{
		Filters.Add(filter.Column, filter);
	}

	/// <summary>
	///		Obtiene el filtro asociado a una columna
	/// </summary>
	internal ParquetFilter? GetFilter(string column)
	{
		if (Filters.TryGetValue(column, out ParquetFilter? filter) && filter.Condition != ParquetFilter.ConditionType.NoCondition)
			return filter;
		else
			return null;
	}

	/// <summary>
	///		Evalúa los filtros sobre una columna
	/// </summary>
	internal bool Evaluate(string column, object? value)
	{
		ParquetFilter? filter = GetFilter(column);

			if (filter is null)
				return true;
			else
				return filter.Condition switch
					{
						ParquetFilter.ConditionType.Contains => EvaluateContains(value?.ToString() ?? string.Empty, 
																				 filter.Value1?.ToString() ?? string.Empty),
						ParquetFilter.ConditionType.Between => false,
						ParquetFilter.ConditionType.In => false,
						_ => Evaluate(filter.Condition, value, filter.Value1)
					};
	}

	/// <summary>
	///		Evalúa la condición
	/// </summary>
	private bool Evaluate(ParquetFilter.ConditionType conditionType, object? value, object? filterValue)
	{
		int comparison = EvaluateCompare(value, filterValue);

			return conditionType switch
					{
						ParquetFilter.ConditionType.Equals => comparison == 0,
						ParquetFilter.ConditionType.Less => comparison < 0,
						ParquetFilter.ConditionType.LessOrEqual => comparison <= 0,
						ParquetFilter.ConditionType.Greater => comparison > 0,
						ParquetFilter.ConditionType.GreaterOrEqual => comparison >= 0,
						ParquetFilter.ConditionType.Distinct => comparison != 0,
						_ => false
					};
	}

	/// <summary>
	///		Evalúa la condición compare
	///		* Menor que cero => value menor que filterValue
	///		* Cero => value igual a filterValue
	///		* Mayor que cero => value mayor que filterValue
	/// </summary>
	private int EvaluateCompare(object? value, object? filterValue)
	{
		if (value is null && filterValue is null)
			return 0;
		else if (value is null)
			return -1;
		else if (value is double || value is decimal || value is float)
			return ((value as double?) ?? 0).CompareTo((filterValue as double?) ?? 0);
		else if (value is int || value is byte || value is short || value is long)
			return ((value as long?) ?? 0).CompareTo((filterValue as long?) ?? 0);
		else if (value is DateTime)
			return ((value as DateTime?) ?? DateTime.UtcNow).CompareTo((filterValue as DateTime?) ?? DateTime.UtcNow);
		else 
			return (value?.ToString() ?? string.Empty).ToUpperInvariant().CompareTo((filterValue?.ToString() ?? string.Empty).ToUpperInvariant());
	}

	/// <summary>
	///		Evalúa la condición que indica si contiene el valor
	/// </summary>
	private bool EvaluateContains(string value, string filterValue) => value.IndexOf(filterValue, StringComparison.CurrentCultureIgnoreCase) >= 0;

	/// <summary>
	///		Número de filtros
	/// </summary>
	public int Count => Filters.Count;

	/// <summary>
	///		Filtros
	/// </summary>
	private Dictionary<string, ParquetFilter> Filters { get; } = new Dictionary<string, ParquetFilter>(StringComparer.InvariantCulture);
}