using System;
using System.Collections.Generic;
using System.Data;
using Parquet;
using Parquet.Data;

namespace Bau.Libraries.LibParquetFiles.Writers.Models
{
	/// <summary>
	///		Datos del esquema de un archivo
	/// </summary>
	internal class ParquetFileModel : IDisposable
	{
		// Variables privadas
		private readonly TimeZoneInfo _timeZoneCentral;

		internal ParquetFileModel(int rowGroupSize)
		{
			// Asigna las propiedades
			RowGroupSize = rowGroupSize;
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
		///		Abre el archivo parquet y lo prepara para escribir los datos
		/// </summary>
		internal void Open(System.IO.Stream stream, IDataReader reader)
		{
			// Asigna las columnas al esquema
			AssignColumnsSchema(reader);
			// Crea el generador de parquet
			Writer = new Parquet.ParquetWriter(GetParquetSchema(), stream);
			// Establece el método de compresión
			//? No asigna el nivel de compresión: deja el predeterminado para el método
			Writer.CompressionMethod = CompressionMethod.Snappy;
		}

		/// <summary>
		///		Asigna las columnas asociadas al <see cref="IDataReader"/> a la caché del archivo
		/// </summary>
		private void AssignColumnsSchema(IDataReader reader)
		{
			// Borra las columnas que hubiera
			Columns.Clear();
			// Obtiene el esquema del dataReader
			for (int index = 0; index < reader.FieldCount; index++)
			{
				if (!string.IsNullOrWhiteSpace(reader.GetName(index)))
					Columns.Add(new ParquetColumnModel(GetColumnSchemaType(reader.GetFieldType(index)), reader.GetName(index), RowGroupSize));
				else
					Columns.Add(new ParquetColumnModel(GetColumnSchemaType(reader.GetFieldType(index)), $"Column{index}", RowGroupSize));
			}
		}

		/// <summary>
		///		Obtiene el esquema Parquet a partir del dataReader
		/// </summary>
		private Schema GetParquetSchema()
		{
			Field[] fields = new Field[Columns.Count];

				// Obtiene los campos (indica que todos admiten nulos)
				for (int index = 0; index < Columns.Count; index++)
					fields[index] = Columns[index].ParquetField;
				// Devuelve la colección de campos
				return new Schema(fields);
		}

		/// <summary>
		///		Obtiene el tipo de columna
		/// </summary>
		private ParquetColumnModel.FieldType GetColumnSchemaType(Type dataType)
		{
			if (IsDataType(dataType, "byte[]")) // ... no vamos a convertir los arrays de bytes
				return ParquetColumnModel.FieldType.Unknown;
			else if (IsDataType(dataType, "int64"))
				return ParquetColumnModel.FieldType.Long;
			else if (IsDataType(dataType, "byte"))
				return ParquetColumnModel.FieldType.Byte;
			else if (IsDataType(dataType, "int")) // int, int16, int32, int64
				return ParquetColumnModel.FieldType.Integer;
			else if (IsDataType(dataType, "decimal"))
				return ParquetColumnModel.FieldType.Decimal;
			else if (IsDataType(dataType, "double") || IsDataType(dataType, "float"))
				return ParquetColumnModel.FieldType.Double;
			else if (IsDataType(dataType, "date"))
				return ParquetColumnModel.FieldType.DateTime;
			else if (IsDataType(dataType, "bool"))
				return ParquetColumnModel.FieldType.Boolean;
			else if (IsDataType(dataType, "Guid"))
				return ParquetColumnModel.FieldType.Guid;
			else
				return ParquetColumnModel.FieldType.String;
		}

		/// <summary>
		///		Comprueba si un nombre de tipo contiene un valor determinado
		/// </summary>
		private bool IsDataType(Type dataType, string search)
		{
			return dataType.FullName.IndexOf("." + search, StringComparison.CurrentCultureIgnoreCase) >= 0;
		}

		/// <summary>
		///		Lee un registro
		/// </summary>
		internal void ReadRecord(IDataReader reader)
		{
			// Lee los datos del registro
			ReadData(reader);
			// Escribe la caché en el archivo si se ha superado el número máximo de filas
			if (Records >= RowGroupSize)
				Flush();
		}

		/// <summary>
		///		Lee los datos del registro
		/// </summary>
		private void ReadData(IDataReader reader)
		{
			for (int index = 0; index < Columns.Count; index++)
				if (!reader.IsDBNull(index))
				{
					switch (Columns[index].Type)
					{
						case ParquetColumnModel.FieldType.Boolean:
								Columns[index].AddBool(reader.GetBoolean(index));
							break;
						case ParquetColumnModel.FieldType.Byte:
								Columns[index].AddByte(reader.GetByte(index));
							break;
						case ParquetColumnModel.FieldType.DateTime:
								DateTime date = (DateTime) reader.GetValue(index);

									// Añade la fecha convertida
									Columns[index].AddDate(new DateTimeOffset(date, _timeZoneCentral.GetUtcOffset(date)));
							break;
						case ParquetColumnModel.FieldType.Decimal:
								Columns[index].AddDecimal(reader.GetDecimal(index));
							break;
						case ParquetColumnModel.FieldType.Double:
								Columns[index].AddDouble(reader.GetDouble(index));
							break;
						case ParquetColumnModel.FieldType.Integer:
								Columns[index].AddInteger(reader.GetInt32(index));
							break;
						case ParquetColumnModel.FieldType.Long:
								Columns[index].AddLong(reader.GetInt64(index));
							break;
						case ParquetColumnModel.FieldType.Guid:
								Columns[index].AddString(reader.GetGuid(index).ToString());
							break;
						default:
								Columns[index].AddString(reader.GetString(index));
							break;
					}
				}
				else
					Columns[index].AddNull();
		}

		/// <summary>
		///		Libera la caché
		/// </summary>
		internal void Flush()
		{
			// Graba los datos que tenía en memoria
			if (Writer != null && Columns.Count > 0 && Columns[0].Count > 0)
				using (ParquetRowGroupWriter groupWriter = Writer.CreateRowGroup())
				{
					for (int index = 0; index < Columns.Count; index++)
						groupWriter.WriteColumn(Columns[index].ConvertToParquet());
				}
			// Limpia los valores que había hasta ahora
			for (int index = 0; index < Columns.Count; index++)
				Columns[index].Clear();
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (!Disposed)
			{
				// Si se está liberando la memoria
				if (disposing && Writer != null)
				{
					// Envía los datos sobrantes
					Flush();
					// Libera el stream
					//? Se tiene que hacer primero el Dispose del Writer para que la librería escriba el pie del archivo, no se puede ponerlo a null directamente
					Writer.Dispose();
					Writer = null;
				}
				// Liberar los recursos no administrados (objetos no administrados) y reemplazar el finalizador
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
			GC.SuppressFinalize(this);
		}

		/// <summary>
		///		Columnas del archivo
		/// </summary>
		private List<ParquetColumnModel> Columns { get; } = new List<ParquetColumnModel>();

		/// <summary>
		///		Generador del archivo parquet
		/// </summary>
		private Parquet.ParquetWriter Writer { get; set; }

		/// <summary>
		///		Tamaño del grupo de filas
		/// </summary>
		private int RowGroupSize { get; }

		/// <summary>
		///		Número de registros en la caché de escritura
		/// </summary>
		internal int Records
		{
			get { return Columns[0].Count; }
		}

		/// <summary>
		///		Indica si se ha liberado la memoria
		/// </summary>
		public bool Disposed { get; private set; }
	}
}
