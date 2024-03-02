namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		Generador de datos
/// </summary>
internal class DataGenerator
{
	private static List<Guid> guids = new() {
												new Guid("DC340CF2-331E-4B58-9F96-B5009EAA8987"),
												new Guid("E649461F-F628-4D2B-B9C8-2CB7A5D07E0A"),
												new Guid("90E6AB79-C2AF-4A61-99EA-BCD3FBE380EA"),
												new Guid("360C9866-F082-4550-A8D1-D21F72316C42"),
												new Guid("1A2BBB07-1D79-4131-A989-CFE607CAE8C8")
											};

	/// <summary>
	///		Crea una lista de ventas
	/// </summary>
	internal List<Sale> Create(DateTime start, int count)
	{
		List<Sale> sales = new();

			// Genera la lista
			for (int index = 0; index < count; index++)
				sales.Add(new Sale(guids[index % guids.Count], $"Product {index}", start.AddMinutes(index), 1.3 * index, index));
			// Devuelve la lista
			return sales;
	}
}
