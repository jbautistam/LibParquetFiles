using System;

namespace Bau.Libraries.LibParquetFiles.Writers.Models
{
	/// <summary>
	///		Array de valores
	/// </summary>
	internal class ColumnArrayModel<TypeData>
	{
		// Variables privadas
		internal TypeData[] _values;

		internal ColumnArrayModel(int maxValues)
		{
			_values = new TypeData[maxValues];
		}

		/// <summary>
		///		Añade un valor
		/// </summary>
		internal void Add(TypeData value)
		{
			// Asigna el valor
			_values[Count] = value;
			// Incrementa el número de registros
			Count++;
		}

		/// <summary>
		///		Convierte los datos en un array
		/// </summary>
		internal Array GetArrayData()
		{
			return _values[0..Count];
		}

		/// <summary>
		///		Número de registros
		/// </summary>
		internal int Count { get; private set; }
	}
}
