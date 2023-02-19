using System;
using Parquet.Data;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Writers.Models
{
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
		private string[] _stringValues;
		private int?[] _intValues;
		private DateTimeOffset?[] _dateTimeValues;
		private long?[] _longValues;
		private double?[] _doubleValues;
		private decimal?[] _decimalValues;
		private bool?[] _boolValues;
		private byte?[] _byteValues;

		internal ParquetColumnModel(FieldType fieldType, string name, int maxValues)
		{
			// Asigna las propiedades
			Type = fieldType;
			Name = name;
			// Crea el campo parquet
			if (Type == FieldType.DateTime)
				ParquetField = new DateTimeDataField(Name, DateTimeFormat.Date);
			else
				ParquetField = new DataField(Name, ConvertType(Type), true);
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
						_dateTimeValues = new DateTimeOffset?[maxValues];
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
		internal DataColumn ConvertToParquet()
		{
			return new DataColumn(ParquetField, GetArrayData());
		}

		/// <summary>
		///		Convierte el tipo de datos
		/// </summary>
		private Type ConvertType(FieldType type)
		{
			switch (type)
			{
				case FieldType.Boolean:
					return typeof(bool);
				case FieldType.Decimal:
					return typeof(decimal);
				case FieldType.Double:
					return typeof(double);
				case FieldType.Byte: // ... los byte no se transforman en enteros por un problema en el intérprete de Spark
					return typeof(byte);
				case FieldType.Integer:
					return typeof(int);
				case FieldType.Long:
					return typeof(long);
				default:
					return typeof(string);
			}
		}

		/// <summary>
		///		Añade una fecha
		/// </summary>
		internal void AddDate(DateTimeOffset value)
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
			switch (Type)
			{
				case FieldType.Boolean:
					return _boolValues[0..Count];
				case FieldType.Byte:
					return _byteValues[0..Count];
				case FieldType.DateTime:
					return _dateTimeValues[0..Count];
				case FieldType.Decimal:
					return _decimalValues[0..Count];
				case FieldType.Double:
					return _doubleValues[0..Count];
				case FieldType.Integer:
					return _intValues[0..Count];
				case FieldType.Long:
					return _longValues[0..Count];
				default:
					return _stringValues[0..Count];
			}
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
}
