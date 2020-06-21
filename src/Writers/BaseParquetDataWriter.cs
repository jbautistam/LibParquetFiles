using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Base para un escritor con archivos Parquet
	/// </summary>
	public abstract class BaseParquetDataWriter
	{
		// Eventos públicos
		public event EventHandler<EventArguments.AffectedEvntArgs> Progress;
		/// <summary>
		///		Tipo del campo
		/// </summary>
		protected enum FieldType
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
			String
		}
		// Variables privadas
		private readonly TimeZoneInfo _timeZoneCentral;

		protected BaseParquetDataWriter(string fileName, int notifyAfter = 200_000)
		{
			// Asigna las propiedades
			FileName = fileName;
			NotifyAfter = notifyAfter;
			// Obtiene la información de zona horaria
			try
			{
				_timeZoneCentral = TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time");
			}
			catch
			{
				_timeZoneCentral = TimeZoneInfo.Local;
			}
		}

		/// <summary>
		///		Escribe un grupo de filas en el archivo
		/// </summary>
		protected void FlushRowGroup(Parquet.ParquetWriter writer, Table table)
		{
			if (table.Count > 0)
				using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup())
				{
					rowGroupWriter.Write(table);
				}
		}

		/// <summary>
		///		Carga el esquema
		/// </summary>
		protected void ReadSchema(IDataReader reader)
		{
			Columns = GetColumnsSchema(reader);
			Schema = GetParquetSchema(Columns);
		}

		/// <summary>
		///		Obtiene las columnas asociadas al <see cref="IDataReader"/>
		/// </summary>
		private List<(string name, FieldType  type)> GetColumnsSchema(IDataReader reader)
		{
			List<(string, FieldType)> columns = new List<(string, FieldType)>();

				// Obtiene el esquema del dataReader
				for (int index = 0; index < reader.FieldCount; index++)
				{
					if (!string.IsNullOrWhiteSpace(reader.GetName(index)))
						columns.Add((reader.GetName(index), GetColumnSchemaType(reader.GetFieldType(index))));
					else
						columns.Add(($"Column{index}", GetColumnSchemaType(reader.GetFieldType(index))));
				}
				// Devuelve la colección de columnas
				return columns;
		}

		/// <summary>
		///		Obtiene el esquema Parquet a partir del dataReader
		/// </summary>
		private Schema GetParquetSchema(List<(string name, FieldType type)> columns)
		{
			Field[] fields = new Field[columns.Count];

				// Obtiene los campos (indica que todos admiten nulos)
				for (int index = 0; index < columns.Count; index++)
					if (columns[index].type == FieldType.DateTime)
						fields[index] = new DateTimeDataField(columns[index].name, DateTimeFormat.Date);
					else
						fields[index] = new DataField(columns[index].name, ConvertType(columns[index].type), true);
				// Devuelve la colección de campos
				return new Schema(fields);
		}

		/// <summary>
		///		Obtiene el tipo de columna
		/// </summary>
		private FieldType GetColumnSchemaType(Type dataType)
		{
			if (IsDataType(dataType, "byte[]")) // ... no vamos a convertir los arrays de bytes
				return FieldType.Unknown;
			else if (IsDataType(dataType, "int64"))
				return FieldType.Long;
			else if (IsDataType(dataType, "byte"))
				return FieldType.Byte;
			else if (IsDataType(dataType, "int")) // int, int16, int32, int64
				return FieldType.Integer;
			else if (IsDataType(dataType, "decimal"))
				return FieldType.Decimal;
			else if (IsDataType(dataType, "double") || IsDataType(dataType, "float"))
				return FieldType.Double;
			else if (IsDataType(dataType, "date"))
				return FieldType.DateTime;
			else if (IsDataType(dataType, "bool"))
				return FieldType.Boolean;
			else
				return FieldType.String;
		}

		/// <summary>
		///		Comprueba si un nombre de tipo contiene un valor determinado
		/// </summary>
		private bool IsDataType(Type dataType, string search)
		{
			return dataType.FullName.IndexOf("." + search, StringComparison.CurrentCultureIgnoreCase) >= 0;
		}

		/// <summary>
		///		Convierte el tipo de datos
		/// </summary>
		private DataType ConvertType(FieldType type)
		{
			switch (type)
			{
				case FieldType.Boolean:
					return DataType.Boolean;
				case FieldType.Decimal:
					return DataType.Decimal;
				case FieldType.Double:
					return DataType.Double;
				case FieldType.Byte: // ... los byte no se transforman en enteros por un problema en el intérprete de Spark
					//TODO: Comprobar si esto sigue siendo necesario
					// return DataType.Byte;
				case FieldType.Integer:
					return DataType.Int32;
				case FieldType.Long:
					return DataType.Int64;
				default:
					return DataType.String;
			}
		}

		/// <summary>
		///		Convierte los datos de un campo
		/// </summary>
		protected Row ConvertData(IDataReader reader)
		{
			List<object> values = new List<object>();

				// Obtiene los campos del registro
				for (int index = 0; index < Columns.Count; index++)
				{
					object value;

						// Obtiene el valor del campo
						if (!reader.IsDBNull(index))
							value = reader.GetValue(index);
						else
							value = null;
						// Convierte el dato del campo
						switch (Columns[index].type)
						{
							case FieldType.Boolean:
									values.Add(value as bool?);
								break;
							case FieldType.Byte:
									values.Add((int) (value as byte?));
								break;
							case FieldType.Integer:
									values.Add(value as int?);
								break;
							case FieldType.Long:
									values.Add(value as long?);
								break;
							case FieldType.Decimal:
									values.Add(value as decimal?);
								break;
							case FieldType.Double:
									values.Add(value as double?);
								break;
							case FieldType.DateTime:
									if (value == null)
										values.Add(null);
									else
									{
										DateTime date = Convert.ToDateTime(value);

											values.Add(new DateTimeOffset(date, _timeZoneCentral.GetUtcOffset(date)));
									}
								break;
							default:
									values.Add(value?.ToString());
								break;
						}
				}
				// Devuelve la fila de valores
				return new Row(values);
		}

		/// <summary>
		///		Lanza el evento de progreso
		/// </summary>
		protected void RaiseProgressEvent(long records)
		{
			if (NotifyAfter > 0 && records % NotifyAfter == 0)
				Progress?.Invoke(this, new EventArguments.AffectedEvntArgs(records));
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (!Disposed)
			{
				// Libera la memoria
				if (disposing)
				{
					// TODO: elimine el estado administrado (objetos administrados).
				}
				// Indica que se ha liberado la memoria
				Disposed = true;
			}
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}

		/// <summary>
		///		Nombre de archivo
		/// </summary>
		public string FileName { get; }

		/// <summary>
		///		Número de registros después de los que se debe notificar
		/// </summary>
		public int NotifyAfter { get; }

		/// <summary>
		///		Columnas del DataReaded
		/// </summary>
		protected List<(string name, FieldType type)> Columns { get; private set; }

		/// <summary>
		///		Esquema del archivo parquet
		/// </summary>
		protected Schema Schema { get; private set; }

		/// <summary>
		///		Indica si se ha liberado la memoria
		/// </summary>
		public bool Disposed { get; private set; }
	}
}