using System.Data;
using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Readers;
using Bau.Libraries.LibParquetFiles.Writers;
using Parquet;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="ParquetDataTableWriter"/> (sin ninguna prueba previa)
/// </summary>
public class ParquetDataTableWriter_Should
{
	/// <summary>
	///		Comprueba que se puede escribir un <see cref="DataTable"/>
	/// </summary>
	[Fact]
	public async Task write_a_data_table()
	{
		DataTable table = DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1, 2, 3 });

		using TempFile file = new();

			await using (ParquetDataTableWriter writer = new(10))
			{
				writer.Open(file.FileName);
				await writer.WriteAsync(table, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> values = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						values.Add((int) reader.GetValue(0));
					values.Should().Equal(1, 2, 3);
			}
	}

	/// <summary>
	///		Comprueba el escenario que justifica esta clase: exportar varias páginas de datos sucesivas sobre el
	///	mismo archivo. El esquema lo fija la primera tabla; las siguientes se escriben posicionalmente contra él
	/// </summary>
	[Fact]
	public async Task write_several_data_tables_into_the_same_file()
	{
		DataTable firstPage = DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1, 2, 3 });
		DataTable secondPage = DataTableBuilder.CreateSingleColumn("Value", new int?[] { 4, 5 });

		using TempFile file = new();

			await using (ParquetDataTableWriter writer = new(10))
			{
				writer.Open(file.FileName);
				await writer.WriteAsync(firstPage, TestContext.Current.CancellationToken);
				await writer.WriteAsync(secondPage, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> values = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						values.Add((int) reader.GetValue(0));
					values.Should().Equal(1, 2, 3, 4, 5);
			}
	}

	/// <summary>
	///		Comprueba que las filas se reparten en grupos según el tamaño configurado
	/// </summary>
	[Fact]
	public async Task split_the_rows_into_row_groups()
	{
		DataTable table = DataTableBuilder.CreateSingleColumn("Value", Enumerable.Range(0, 25).Cast<int?>());

		using TempFile file = new();

			await using (ParquetDataTableWriter writer = new(10))
			{
				writer.Open(file.FileName);
				await writer.WriteAsync(table, TestContext.Current.CancellationToken);
			}
			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				parquetReader.RowGroupCount.Should().Be(3);
			}
	}

	/// <summary>
	///		Comprueba que el pie del archivo (y por tanto un archivo legible) sólo se escribe al liberar el escritor
	/// </summary>
	[Fact]
	public async Task write_the_footer_on_dispose()
	{
		DataTable table = DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1 });

		using TempFile file = new();
		ParquetDataTableWriter writer = new(10);

			writer.Open(file.FileName);
			await writer.WriteAsync(table, TestContext.Current.CancellationToken);
			await writer.DisposeAsync();

			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					(await reader.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
			}
	}

	/// <summary>
	///		Comprueba que se puede grabar explícitamente lo pendiente sin cerrar el escritor, y que se puede seguir
	///	escribiendo después
	/// </summary>
	[Fact]
	public async Task flush_pending_rows_without_closing_the_writer()
	{
		using TempFile file = new();

			await using (ParquetDataTableWriter writer = new(10))
			{
				writer.Open(file.FileName);
				await writer.WriteAsync(DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1, 2 }), TestContext.Current.CancellationToken);
				await writer.FlushAsync(TestContext.Current.CancellationToken);
				await writer.WriteAsync(DataTableBuilder.CreateSingleColumn("Value", new int?[] { 3, 4 }), TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				List<int> values = new();

					await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await reader.ReadAsync(TestContext.Current.CancellationToken))
						values.Add((int) reader.GetValue(0));
					values.Should().Equal(1, 2, 3, 4);
			}
	}

	/// <summary>
	///		Comprueba que escribir antes de abrir el archivo lanza <see cref="InvalidOperationException"/> (antes
	///	lanzaba <see cref="NotImplementedException"/>, un tipo de excepción incorrecto para esta situación)
	/// </summary>
	[Fact]
	public async Task throw_when_writing_before_open()
	{
		DataTable table = DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1 });
		ParquetDataTableWriter writer = new(10);

			Func<Task> act = () => writer.WriteAsync(table, TestContext.Current.CancellationToken);

			await act.Should().ThrowAsync<InvalidOperationException>();
	}

	/// <summary>
	///		Comprueba que grabar antes de abrir el archivo lanza <see cref="InvalidOperationException"/>
	/// </summary>
	[Fact]
	public async Task throw_when_flushing_before_open()
	{
		ParquetDataTableWriter writer = new(10);

			Func<Task> act = () => writer.FlushAsync(TestContext.Current.CancellationToken);

			await act.Should().ThrowAsync<InvalidOperationException>();
	}

	/// <summary>
	///		Comprueba que abrir el archivo después de liberar el escritor lanza <see cref="ObjectDisposedException"/>
	/// </summary>
	[Fact]
	public async Task throw_when_opening_after_dispose()
	{
		using TempFile file = new();
		ParquetDataTableWriter writer = new(10);

			await writer.DisposeAsync();

			Action act = () => writer.Open(file.FileName);

			act.Should().Throw<ObjectDisposedException>();
	}

	/// <summary>
	///		Comprueba que abrir el archivo dos veces sin cerrar lanza, en lugar de fugar el archivo anterior
	/// </summary>
	[Fact]
	public async Task throw_when_opening_twice()
	{
		using TempFile file = new();
		await using ParquetDataTableWriter writer = new(10);

			writer.Open(file.FileName);

			Action act = () => writer.Open(file.FileName);

			act.Should().Throw<InvalidOperationException>();
	}

	/// <summary>
	///		Comprueba que un <see cref="DataTable"/> sin columnas lanza en lugar de fallar dentro de Parquet.Net
	/// </summary>
	[Fact]
	public async Task throw_when_the_table_has_no_columns()
	{
		using TempFile file = new();
		await using ParquetDataTableWriter writer = new(10);

			writer.Open(file.FileName);

			Func<Task> act = () => writer.WriteAsync(DataTableBuilder.CreateEmpty(), TestContext.Current.CancellationToken);

			await act.Should().ThrowAsync<ArgumentException>();
	}

	/// <summary>
	///		Comprueba que se puede llamar a DisposeAsync dos veces sin que lance
	/// </summary>
	[Fact]
	public async Task be_safe_to_dispose_twice()
	{
		using TempFile file = new();
		ParquetDataTableWriter writer = new(10);

			writer.Open(file.FileName);
			await writer.WriteAsync(DataTableBuilder.CreateSingleColumn("Value", new int?[] { 1 }), TestContext.Current.CancellationToken);
			await writer.DisposeAsync();

			Func<Task> act = () => writer.DisposeAsync().AsTask();

			await act.Should().NotThrowAsync();
	}
}
