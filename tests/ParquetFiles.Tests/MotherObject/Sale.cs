namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		Objeto de ventas
/// </summary>
internal class Sale
{
	internal Sale(Guid id, string productId, DateTime date, double price, int quantity)
	{
		Id = id;
		ProductId =	productId;
		Date = date;
		Price = price;
		Quantity = quantity;
	}

	/// <summary>
	///		Id
	/// </summary>
	internal Guid Id { get; }

	/// <summary>
	///		Códigod de producto
	/// </summary>
	internal string ProductId { get; }

	/// <summary>
	///		Fecha
	/// </summary>
	internal DateTime Date { get; } = DateTime.UtcNow;

	/// <summary>
	///		Precio
	/// </summary>
	internal double Price { get; }

	/// <summary>
	///		Cantidad
	/// </summary>
	internal int Quantity { get; }
}
