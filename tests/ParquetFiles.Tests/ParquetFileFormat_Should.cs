using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Writers;
using Parquet;
using Parquet.Meta;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas que abren los archivos generados con la API cruda de Parquet.Net (no con nuestro propio
///	<see cref="AnalyticAlways.ParquetFiles.Toolbelt.Readers.ParquetDataReader"/>), para no validar el escritor
///	únicamente contra nuestro propio lector: una suite que sólo enfrentara ambas mitades entre sí podría pasar
///	estando las dos mal
/// </summary>
public class ParquetFileFormat_Should
{
	/// <summary>
	///		Comprueba que todas las columnas se comprimen con Snappy (hoy fijo en <c>ParquetFileModel.OpenAsync</c>,
	///	sin ninguna opción pública para cambiarlo)
	/// </summary>
	[Fact]
	public async Task compress_every_column_with_snappy()
	{
		using TempFile file = new();

			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", new int?[] { 1, 2, 3 }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				foreach (RowGroup rowGroup in parquetReader.Metadata!.RowGroups)
					foreach (ColumnChunk column in rowGroup.Columns)
						column.MetaData!.Codec.Should().Be(CompressionCodec.SNAPPY);
			}
	}

	/// <summary>
	///		Comprueba que todas las columnas se declaran opcionales en el esquema físico, incluso las que provienen
	///	de un origen que no admite nulos
	/// </summary>
	[Fact]
	public async Task declare_every_column_as_optional_even_when_the_source_is_not_nullable()
	{
		using TempFile file = new();

			using (FakeDataReader reader = CreateSampleReader())
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				// El primer elemento del esquema físico es la raíz del mensaje, el resto son las columnas
				foreach (SchemaElement element in parquetReader.Metadata!.Schema.Skip(1))
					element.RepetitionType.Should().Be(FieldRepetitionType.OPTIONAL);
			}
	}

	/// <summary>
	///		Comprueba que se generan tantos grupos de filas como corresponde al tamaño configurado
	/// </summary>
	[Fact]
	public async Task write_the_expected_number_of_row_groups()
	{
		using TempFile file = new();

			// 50 filas con un tamaño de grupo de 20: 2 grupos completos + 1 grupo parcial de 10
			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", Enumerable.Range(0, 50).Cast<int?>()))
			{
				ParquetDataWriter writer = new(20);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				parquetReader.RowGroupCount.Should().Be(3);
			}
	}

	/// <summary>
	///		Tipos físicos de Parquet esperados para cada tipo CLR de origen (comprobado experimentalmente contra
	///	Parquet.Net 6.0.3)
	/// </summary>
	public static IEnumerable<object[]> PhysicalTypes()
	{
		yield return new object[] { typeof(bool), true, Parquet.Meta.Type.BOOLEAN };
		yield return new object[] { typeof(byte), (byte) 1, Parquet.Meta.Type.INT32 };
		yield return new object[] { typeof(int), 1, Parquet.Meta.Type.INT32 };
		yield return new object[] { typeof(long), 1L, Parquet.Meta.Type.INT64 };
		yield return new object[] { typeof(double), 1.0, Parquet.Meta.Type.DOUBLE };
		yield return new object[] { typeof(decimal), 1.5m, Parquet.Meta.Type.FIXED_LEN_BYTE_ARRAY };
		yield return new object[] { typeof(DateTime), DateTime.UtcNow, Parquet.Meta.Type.INT96 };
		yield return new object[] { typeof(Guid), Guid.NewGuid(), Parquet.Meta.Type.FIXED_LEN_BYTE_ARRAY };
		yield return new object[] { typeof(string), "a", Parquet.Meta.Type.BYTE_ARRAY };
		yield return new object[] { typeof(byte[]), new byte[] { 1, 2 }, Parquet.Meta.Type.BYTE_ARRAY };
		yield return new object[] { typeof(TimeSpan), TimeSpan.FromHours(1), Parquet.Meta.Type.INT32 };
	}

	/// <summary>
	///		Comprueba el tipo físico de Parquet que corresponde a cada tipo CLR de origen
	/// </summary>
	[Theory]
	[MemberData(nameof(PhysicalTypes))]
	public async Task map_clr_types_to_the_expected_parquet_physical_type(System.Type sourceType, object value, Parquet.Meta.Type expectedPhysicalType)
	{
		using TempFile file = new();

			using (FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", sourceType, new object?[] { value }))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);
			}
			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				ColumnChunk column = parquetReader.Metadata!.RowGroups[0].Columns[0];

					column.MetaData!.Type.Should().Be(expectedPhysicalType);
			}
	}

	/// <summary>
	///		Crea un lector de varias columnas y varios tipos para las pruebas de formato
	/// </summary>
	private static FakeDataReader CreateSampleReader()
	{
		(string Name, System.Type Type)[] columns =
			{
				("Id", typeof(Guid)),
				("Name", typeof(string)),
				("Date", typeof(DateTime)),
				("Amount", typeof(decimal)),
				("Active", typeof(bool))
			};
		List<object?[]> rows = new()
			{
				new object?[] { Guid.NewGuid(), "Foo", DateTime.UtcNow, 12.5m, true }
			};

		return new FakeDataReader(columns, rows);
	}
}
