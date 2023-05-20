using Parquet.Data;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Writers.Models;

/// <summary>
///		Modelo de escritura de una columna
/// </summary>
internal class ParquetColumnModel
{
	/// <summary>
	///		Tipo del campo
	/// </summary>
	internal enum FieldType
	{
		/// <summary>Desconocido. No se debería utilizar</summary>
		Unknown,
		/// <summary>Valor lógico</summary>
		Boolean,
		/// <summary>Fecha / hora</summary>
		DateTime,
		/// <summary>Byte</summary>
		Byte,
		/// <summary>Entero</summary>
		Integer,
		/// <summary>Entero largo</summary>
		Long,
		/// <summary>Decimal</summary>
		Decimal,
		/// <summary>doble</summary>
		Double,
		/// <summary>Cadena</summary>
		String,
		/// <summary>Guid: en las grabaciones se tratará como cadena</summary>
		Guid
	}
	// Variables privadas
	private string?[] _stringValues = default!;
	private int?[] _intValues = default!;
	private DateTime?[] _dateTimeValues = default!;
	private long?[] _longValues = default!;
	private double?[] _doubleValues = default!;
	private decimal?[] _decimalValues = default!;
	private bool?[] _boolValues = default!;
	private byte?[] _byteValues = default!;

	internal ParquetColumnModel(FieldType fieldType, string name, int maxValues)
	{
		// Asigna las propiedades
		Type = fieldType;
		Name = name;
		// Crea el campo parquet
		ParquetField = ConvertType(fieldType, name);
		// Crea los arrays de valores
		switch (fieldType)
		{
			case FieldType.Boolean:
					_boolValues = new bool?[maxValues];
				break;
			case FieldType.Byte:
					_byteValues = new byte?[maxValues];
				break;
			case FieldType.DateTime:
					_dateTimeValues = new DateTime?[maxValues];
				break;
			case FieldType.Decimal:
					_decimalValues = new decimal?[maxValues];
				break;
			case FieldType.Double:
					_doubleValues = new double?[maxValues];
				break;
			case FieldType.Integer:
					_intValues = new int?[maxValues];
				break;
			case FieldType.Long:
					_longValues = new long?[maxValues];
				break;
			default:
					_stringValues = new string[maxValues];
				break;
		}
	}

	/// <summary>
	///		Añade un nulo
	/// </summary>
	internal void AddNull()
	{
		// Asigna el valor
		switch (Type)
		{
			case FieldType.Boolean:
					_boolValues[Count] = null;
				break;
			case FieldType.Byte:
					_byteValues[Count] = null;
				break;
			case FieldType.DateTime:
					_dateTimeValues[Count] = null;
				break;
			case FieldType.Decimal:
					_decimalValues[Count] = null;
				break;
			case FieldType.Double:
					_doubleValues[Count] = null;
				break;
			case FieldType.Integer:
					_intValues[Count] = null;
				break;
			case FieldType.Long:
					_longValues[Count] = null;
				break;
			default:
					_stringValues[Count] = null;
				break;
		}
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Convierte los valores de la columna a Parquet
	/// </summary>
	internal DataColumn ConvertToParquet() => new DataColumn(ParquetField, GetArrayData());

	/// <summary>
	///		Convierte el tipo de datos
	/// </summary>
	private DataField ConvertType(FieldType type, string name)
	{
		return type switch
			{
				FieldType.Boolean => new DataField<bool?>(name),
				FieldType.Decimal => new DataField<decimal?>(name),
				FieldType.Double => new DataField<double?>(name),
				FieldType.Byte => new DataField<byte?>(name),
				FieldType.Integer => new DataField<int?>(name),
				FieldType.Long => new DataField<long?>(name),
				FieldType.DateTime => new DataField<DateTime?>(name),
				_ => new DataField<string?>(name)
			};
	}

	/// <summary>
	///		Añade una fecha
	/// </summary>
	internal void AddDate(DateTime value)
	{
		// Asigna el valor
		_dateTimeValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un decimal
	/// </summary>
	internal void AddDecimal(decimal value)
	{
		// Asigna el valor
		_decimalValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un byte
	/// </summary>
	internal void AddByte(byte value)
	{
		// Asigna el valor
		_byteValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un doble
	/// </summary>
	internal void AddDouble(double value)
	{
		// Asigna el valor
		_doubleValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un entero
	/// </summary>
	internal void AddInteger(int value)
	{
		// Asigna el valor
		_intValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un boolean
	/// </summary>
	internal void AddBool(bool value)
	{
		// Asigna el valor
		_boolValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade un entero largo
	/// </summary>
	internal void AddLong(long value)
	{
		// Asigna el valor
		_longValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Añade una cadena
	/// </summary>
	internal void AddString(string value)
	{
		// Asigna el valor
		_stringValues[Count] = value;
		// Incrementa el número de registros
		Count++;
	}

	/// <summary>
	///		Convierte los datos en un array
	/// </summary>
	private Array GetArrayData()
	{
		return Type switch
			{
				FieldType.Boolean => _boolValues[..Count],
				FieldType.Byte => _byteValues[..Count],
				FieldType.DateTime => _dateTimeValues[..Count],
				FieldType.Decimal => _decimalValues[..Count],
				FieldType.Double => _doubleValues[..Count],
				FieldType.Integer => _intValues[..Count],
				FieldType.Long => _longValues[..Count],
				_ => _stringValues[..Count]
			};
	}

	/// <summary>
	///		Limpia los datos
	/// </summary>
	public void Clear()
	{
		Count = 0;
	}

	/// <summary>
	///		Tipo del campo
	/// </summary>
	internal FieldType Type { get; }

	/// <summary>
	///		Nombre del campo
	/// </summary>
	internal string Name { get; }

	/// <summary>
	///		Campo de tipo parquet
	/// </summary>
	internal DataField ParquetField { get; }

	/// <summary>
	///		Obtiene el número de valores de la columan
	/// </summary>
	internal int Count { get; private set; }
}
