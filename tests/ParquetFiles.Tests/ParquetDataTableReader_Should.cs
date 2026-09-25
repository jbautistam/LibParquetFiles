using System.Data;
using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Readers;
using Bau.Libraries.LibParquetFiles.Writers;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="ParquetDataTableReader"/> (sin ninguna prueba previa)
/// </summary>
/// <remarks>
///		Usa dos tipos de fixture a propósito: <c>Data/channels-01.parquet</c> (un archivo real de una sola fila,
///	generado por otra herramienta, con columnas nulas) para el esquema y los nulos; y archivos generados en un
///	directorio temporal, con un número de filas conocido, para poder afirmar recuentos exactos de paginación
/// </remarks>
public class ParquetDataTableReader_Should
{
	/// <summary>
	///		Comprueba que se carga el esquema completo del archivo (12 columnas, comprobado inspeccionando el fixture)
	/// </summary>
	[Fact]
	public async Task load_the_schema_of_the_file()
	{
		ParquetDataTableReader reader = new();

			(DataTable table, long total) = await reader.LoadAsync(GetSampleFilePath(), 1, 10, true, TestContext.Current.CancellationToken);

			table.Columns.Count.Should().Be(12);
			table.Columns["Name"]!.DataType.Should().Be(typeof(string));
			total.Should().Be(1);
	}

	/// <summary>
	///		Comprueba que los valores nulos del archivo se conservan como <see cref="DBNull"/> en el DataTable
	/// </summary>
	[Fact]
	public async Task keep_nulls_as_dbnull_in_the_data_table()
	{
		ParquetDataTableReader reader = new();

			(DataTable table, _) = await reader.LoadAsync(GetSampleFilePath(), 1, 10, true, TestContext.Current.CancellationToken);

			table.Rows[0]["DeleteDate"].Should().Be(DBNull.Value);
			table.Rows[0]["DeleteUser"].Should().Be(DBNull.Value);
			table.Rows[0]["Name"].Should().Be("Tiendas Propias");
	}

	/// <summary>
	///		Comprueba que se carga la primera página de un archivo con varias filas
	/// </summary>
	[Fact]
	public async Task load_the_first_page()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 25);

			ParquetDataTableReader reader = new();
			(DataTable table, _) = await reader.LoadAsync(file.FileName, 1, 10, true, TestContext.Current.CancellationToken);

				table.Rows.Cast<DataRow>().Select(row => (int) row["Value"]).Should().Equal(Enumerable.Range(0, 10));
	}

	/// <summary>
	///		Comprueba que se carga una página intermedia completa
	/// </summary>
	[Fact]
	public async Task load_an_intermediate_page()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 25);

			ParquetDataTableReader reader = new();
			(DataTable table, _) = await reader.LoadAsync(file.FileName, 2, 10, true, TestContext.Current.CancellationToken);

				table.Rows.Cast<DataRow>().Select(row => (int) row["Value"]).Should().Equal(Enumerable.Range(10, 10));
	}

	/// <summary>
	///		Comprueba que la última página, con menos filas que el tamaño de página, se carga completa (parcial)
	/// </summary>
	[Fact]
	public async Task load_a_partial_last_page()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 25);

			ParquetDataTableReader reader = new();
			(DataTable table, _) = await reader.LoadAsync(file.FileName, 3, 10, true, TestContext.Current.CancellationToken);

				table.Rows.Cast<DataRow>().Select(row => (int) row["Value"]).Should().Equal(Enumerable.Range(20, 5));
	}

	/// <summary>
	///		Comprueba que pedir una página más allá del final del archivo devuelve una tabla vacía pero con esquema
	/// </summary>
	[Fact]
	public async Task return_an_empty_table_with_schema_for_a_page_beyond_the_end()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 10);

			ParquetDataTableReader reader = new();
			(DataTable table, _) = await reader.LoadAsync(file.FileName, 5, 10, true, TestContext.Current.CancellationToken);

				table.Columns.Count.Should().Be(1);
				table.Rows.Count.Should().Be(0);
	}

	/// <summary>
	///		Comprueba que, con countRecords a true, se devuelve el número real de filas del archivo (se lee hasta
	///	el final aunque la página solicitada ya esté completa)
	/// </summary>
	[Fact]
	public async Task count_every_record_when_count_records_is_true()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 37);

			ParquetDataTableReader reader = new();
			(_, long total) = await reader.LoadAsync(file.FileName, 1, 10, true, TestContext.Current.CancellationToken);

				total.Should().Be(37);
	}

	/// <summary>
	///		Comprueba que, con countRecords a false, la lectura se detiene en cuanto la página está completa y el
	///	total devuelto es exactamente lo leído (no el total real del archivo)
	/// </summary>
	[Fact]
	public async Task stop_early_when_count_records_is_false()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 37);

			ParquetDataTableReader reader = new();
			(DataTable table, long total) = await reader.LoadAsync(file.FileName, 2, 10, false, TestContext.Current.CancellationToken);

				total.Should().Be(20);
				table.Rows.Count.Should().Be(10);
	}

	/// <summary>
	///		Comprueba que la página debe ser un número positivo (antes, page=0 o negativo daba una página vacía sin
	///	ningún error)
	/// </summary>
	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public async Task throw_when_the_page_is_not_positive(int page)
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 5);

			ParquetDataTableReader reader = new();
			Func<Task> act = () => reader.LoadAsync(file.FileName, page, 10, true, TestContext.Current.CancellationToken);

				await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
	}

	/// <summary>
	///		Comprueba que el número de registros por página debe ser positivo
	/// </summary>
	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public async Task throw_when_records_per_page_is_not_positive(int recordsPerPage)
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 5);

			ParquetDataTableReader reader = new();
			Func<Task> act = () => reader.LoadAsync(file.FileName, 1, recordsPerPage, true, TestContext.Current.CancellationToken);

				await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
	}

	/// <summary>
	///		Comprueba que un archivo sin filas devuelve una tabla con esquema pero sin filas, y el total en cero
	///	(antes, el esquema sólo se añadía dentro del bucle de lectura, así que un archivo sin filas devolvía una
	///	tabla completamente sin columnas)
	/// </summary>
	[Fact]
	public async Task return_a_table_with_schema_and_no_rows_for_a_file_without_records()
	{
		using TempFile file = new();

			await WriteNumberedRowsAsync(file.FileName, 0);

			ParquetDataTableReader reader = new();
			(DataTable table, long total) = await reader.LoadAsync(file.FileName, 1, 10, true, TestContext.Current.CancellationToken);

				table.Columns.Count.Should().Be(1);
				table.Rows.Count.Should().Be(0);
				total.Should().Be(0);
	}

	/// <summary>
	///		Comprueba que cancelar la carga lanza, en lugar de devolver una página parcial en silencio
	/// </summary>
	[Fact]
	public async Task throw_when_cancelled()
	{
		using TempFile file = new();
		using CancellationTokenSource cts = new();

			await WriteNumberedRowsAsync(file.FileName, 5);
			cts.Cancel();

			ParquetDataTableReader reader = new();
			Func<Task> act = () => reader.LoadAsync(file.FileName, 1, 10, true, cts.Token);

				await act.Should().ThrowAsync<OperationCanceledException>();
	}

	/// <summary>
	///		Ruta del archivo de ejemplo generado por otra herramienta (una fila, 12 columnas, con nulos)
	/// </summary>
	private static string GetSampleFilePath() => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Data", "channels-01.parquet");

	/// <summary>
	///		Escribe un archivo con una única columna "Value" con los enteros de 0 a <paramref name="rows"/> - 1
	/// </summary>
	private static async Task WriteNumberedRowsAsync(string fileName, int rows)
	{
		(string, Type)[] columns = { ("Value", typeof(int)) };
		List<object?[]> data = Enumerable.Range(0, rows).Select(i => new object?[] { i }).ToList();

			using FakeDataReader reader = new(columns, data);
			ParquetDataWriter writer = new(1_000);

				await writer.WriteAsync(fileName, reader, TestContext.Current.CancellationToken);
	}
}
