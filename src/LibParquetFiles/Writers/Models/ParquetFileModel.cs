using System.Data;

using Parquet;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Writers.Models;

/// <summary>
///		Datos del esquema de un archivo
/// </summary>
internal class ParquetFileModel : IAsyncDisposable
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
	internal async Task OpenAsync(Stream stream, IDataReader reader, CancellationToken cancellationToken)
	{
		// Asigna las columnas al esquema
		AssignColumnsSchema(reader);
		// Crea el generador de parquet
		Writer = await ParquetWriter.CreateAsync(GetParquetSchema(), stream, cancellationToken: cancellationToken);
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
	private ParquetSchema GetParquetSchema()
	{
		Field[] fields = new Field[Columns.Count];

			// Obtiene los campos (indica que todos admiten nulos)
			for (int index = 0; index < Columns.Count; index++)
				fields[index] = Columns[index].ParquetField;
			// Devuelve la colección de campos
			return new ParquetSchema(fields);
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
		return dataType.FullName?.IndexOf("." + search, StringComparison.CurrentCultureIgnoreCase) >= 0;
	}

	/// <summary>
	///		Lee los datos de un registro y los graba en el archivo
	/// </summary>
	internal async Task WriteRecordAsync(IDataReader reader, CancellationToken cancellationToken)
	{
		// Lee los datos del registro
		ReadData(reader);
		// Escribe la caché en el archivo si se ha superado el número máximo de filas
		if (Records >= RowGroupSize)
			await FlushAsync(cancellationToken);
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
							Columns[index].AddDate((DateTime) reader.GetValue(index));
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
	internal async Task FlushAsync(CancellationToken cancellationToken)
	{
		// Graba los datos que tenía en memoria
		if (Writer is not null && Columns.Count > 0 && Columns[0].Count > 0)
			using (ParquetRowGroupWriter groupWriter = Writer.CreateRowGroup())
			{
				for (int index = 0; index < Columns.Count; index++)
					await groupWriter.WriteColumnAsync(Columns[index].ConvertToParquet(), cancellationToken);
			}
		// Limpia los valores que había hasta ahora
		for (int index = 0; index < Columns.Count; index++)
			Columns[index].Clear();
	}

	/// <summary>
	///		Libera la memoria
	/// </summary>
	public virtual async ValueTask DisposeAsync()
	{
		if (!Disposed)
		{
			// Si se está liberando la memoria
			if (Writer is not null)
			{
				// Envía los datos sobrantes
				await FlushAsync(CancellationToken.None);
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
	///		Columnas del archivo
	/// </summary>
	private List<ParquetColumnModel> Columns { get; } = new();

	/// <summary>
	///		Generador del archivo parquet
	/// </summary>
	private ParquetWriter? Writer { get; set; }

	/// <summary>
	///		Tamaño del grupo de filas
	/// </summary>
	private int RowGroupSize { get; }

	/// <summary>
	///		Número de registros en la caché de escritura
	/// </summary>
	internal int Records => Columns[0].Count;

	/// <summary>
	///		Indica si se ha liberado la memoria
	/// </summary>
	public bool Disposed { get; private set; }
}
