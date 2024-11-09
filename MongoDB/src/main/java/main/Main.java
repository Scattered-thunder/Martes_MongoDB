package main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class Main {

	public static void main(String[] args) throws ParseException{

	// I connect myself to MongoDB Instance
	MongoCollection<Document> collection_Depositos = connectToMongoDB("HarryPractica", "Depositos");
	MongoCollection<Document> collection_Envios = connectToMongoDB("HarryPractica", "Envios");
	MongoCollection<Document> collection_Pedidos = connectToMongoDB("HarryPractica", "Pedidos");
	
	// Add all the data, RUN ONLY ONCE OR YOU WILL GET DUPLICATE ENTRIES, dont forget to run it first lol 
	/*
	agregarDatos(collection_Depositos);
	agregarDatosPedidos(collection_Pedidos);
	agregarDatosEnvios(collection_Envios);
	*/
	
	Ejercicio_1(collection_Depositos, 1);
	Ejercicio_1(collection_Depositos, 2);
	Ejercicio_2(collection_Pedidos,"Arroz");
	Ejercicio_3(collection_Envios, 1);
	//........... Run the rest if you want, i got lazy...they are tested beforehand though
	}


	public static void Ejercicio_10(MongoCollection<Document> Collection, String Deposito) {

		FindIterable<Document> resultado = Collection.find(Filters.elemMatch("contiene.contiene", Filters.eq("deposito",Deposito)));
		
		for(Document currentDocumento: resultado) {
			List<Document> pedidos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentPedido: pedidos) {
				Document currentCliente = (Document) currentPedido.get("cliente");
				List<Document> productos = (List<Document>) currentPedido.get("contiene");
				for(Document currentProducto: productos) {
					if(currentProducto.get("deposito").equals(Deposito)) {
						System.out.println("------------------------- ID Envio: " + currentDocumento.get("idEnvio") + " ---------------------------------\n");
						System.out.println("Fecha del Envio: " + currentDocumento.get("fecha"));
						System.out.println("\n--------------------------ID Pedido: " + currentPedido.get("idPedido") + " --------------------------------\n");
						System.out.println("Cliente: " + currentCliente.get("nombre"));
						System.out.println("Fecha del Pedido: " + currentPedido.get("fecha"));
						System.out.println("\n-----------------------------------------------------------------------\n");
						System.out.println("Producto: " + currentProducto.get("nombre"));
						System.out.println("Origen: " + currentProducto.get("deposito"));	
						System.out.println("\n-----------------------------------------------------------------------\n");
					}
				}
			}
		}
	}
	
	public static void Ejercicio_9(MongoCollection<Document> Collection, String cliente, String Fecha_Ini, String Fecha_Fin) throws ParseException {
		
		SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
		FindIterable<Document> resultado = Collection.find(Filters.and(
				Filters.eq("cliente.nombre", cliente), 
				Filters.gte("fecha", DateFormat.parse(Fecha_Ini)),
				Filters.lte("fecha", DateFormat.parse(Fecha_Fin)))
				);
		
		Integer valorTotalPedidos = 0;
		
		for(Document currentDocumento: resultado) {
			System.out.println("------------------------------------------------------------------------------");
			System.out.println("Pedido ID: " + currentDocumento.get("idPedido"));
			Integer valorPedido = 0;
			List<Document> productos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentProducto: productos) {
				valorPedido = valorPedido + (Integer) currentProducto.get("precio");
			}
			System.out.println("Valor del Pedido: " + valorPedido + " $");
			System.out.println("------------------------------------------------------------------------------");
			valorTotalPedidos = valorTotalPedidos + valorPedido;
			}
		System.out.println("------------------------------------------------------------------------------");
		System.out.println("Valor de todos los pedidos: " + valorTotalPedidos + " $");
		System.out.println("------------------------------------------------------------------------------");
	}
	
	public static void Ejercicio_8(MongoCollection<Document> Collection, String producto) {
		
		FindIterable<Document> resultado = Collection.find(Filters.elemMatch("contiene.contiene",Filters.eq("nombre",producto)));
		
		for(Document currentDocumento: resultado) {
			System.out.println("ID Envio: " + currentDocumento.get("idEnvio"));
			System.out.println("Fecha de Envio: " + currentDocumento.get("fecha"));
			System.out.println("ID Envio: " + currentDocumento.get("estadoEnvio"));
			List<Document> pedidos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentPedido: pedidos) {
				List<Document> productos = (List<Document>) currentPedido.get("contiene");
				for(Document currentProducto: productos) {
					if(currentProducto.get("nombre").equals(producto)) {
						System.out.println("Deposito de Origen: " + currentProducto.get("deposito"));
					}
				}
			}
		}
		
	}
	
	public static void Ejercicio_7(MongoCollection<Document> Collection, String nombre) {
		
		FindIterable<Document> resultado = Collection.find(Filters.eq("cliente.nombre", nombre));
		
		for(Document currentDocumento : resultado) {
			System.out.println("------------------------------------------------------------------------------");
			System.out.println("Pedido ID: " + currentDocumento.get("idPedido"));
			System.out.println("Estado: " + currentDocumento.get("estado"));
			System.out.println("------------------------------------------------------------------------------");
			List<Document> productos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentProducto: productos) {
				System.out.println("Producto: " + currentProducto.get("nombre"));
			}
			System.out.println("------------------------------------------------------------------------------");
		}
	}
	
	public static void Ejercicio_6(MongoCollection<Document> Collection, Integer idDeposito) {
		
		FindIterable<Document> resultado = Collection.find(Filters.eq("idDeposito", idDeposito));
		
		for(Document currentDocumento : resultado) {
			
			System.out.println("Deposito con ID: " + currentDocumento.get("idDeposito"));
			System.out.println(currentDocumento.get("nombre"));
			
			// Set both values to 0
			Integer cantidadProductosTotal = 0;
			Integer valorProductosTotal = 0;
			
			List<Document> productos = (List<Document>) currentDocumento.get("productos");
			
			for(Document currentProduct: productos) {
				//System.out.println("Producto: " + currentProduct.get("nombre"));
				cantidadProductosTotal = cantidadProductosTotal + 1;
				valorProductosTotal = valorProductosTotal + (Integer) currentProduct.get("stock");
			}
			System.out.println("------------------------------------------------------------------------------");
			System.out.println("Cantidad de registros de productos en el depósito: " + cantidadProductosTotal);
			System.out.println("Cantidad de productos total en el depósito: " + valorProductosTotal + " Unidades");
			System.out.println("------------------------------------------------------------------------------");
		}
	}
	
	
	public static void Ejercicio_5(MongoCollection<Document> Collection, Integer idEnvio) {

		FindIterable<Document> resultado = Collection.find(Filters.eq("idEnvio", idEnvio));
		
		for(Document currentDocumento: resultado ) {
			System.out.println("Envio con ID: " + currentDocumento.get("idEnvio"));
			System.out.println("---------Productos------------------------");
			List<Document> pedidos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentPedido : pedidos) {
				List<Document> productos = (List<Document>) currentPedido.get("contiene");
				for(Document currentProducto : productos) {
					System.out.println(currentProducto.getString("nombre"));
				}
			}
			System.out.println("------------------------------------------");
		}
	}
	
	public static void Ejercicio_4(MongoCollection<Document> Collection, String Producto) {
		
		FindIterable<Document> resultado = Collection.find(Filters.elemMatch("contiene.contiene", Filters.eq("nombre",Producto)));
		
		// Set de Depósitos Unicos
		
		Set<String> depositos = new HashSet<>();
		
		// Por cada documento
		
		for(Document currentDocumento: resultado) {
			
			// Por cada pedido del documento
			
			List<Document> pedidos = (List<Document>) currentDocumento.get("contiene");
			for(Document currentPedido: pedidos) {
				
				// Por cada producto del pedido
				
				List<Document> productos = (List<Document>) currentPedido.get("contiene");
				for(Document currentProducto: productos) {
					if(currentProducto.get("nombre").equals(Producto)) {
						depositos.add((String) currentProducto.get("deposito"));
					}
				}
			}
		}
		
		for(String currentDeposito: depositos) {
			System.out.println("Deposito: " + currentDeposito);
		}
	}
	
	public static void Ejercicio_3(MongoCollection<Document> Collection, int idEnvio) {
		FindIterable<Document> resultado = Collection.find(Filters.eq("idEnvio", idEnvio));
		
		for (Document currentDocument : resultado) {
			System.out.println("------------------------------------------------------------");
			System.out.println("ID Envio: "  + currentDocument.get("idEnvio"));
			System.out.println("Fecha: " + currentDocument.get("fecha"));
			System.out.println("Estado: " + currentDocument.get("estadoEnvio"));
			List<Document> pedidos = (List<Document>) currentDocument.get("contiene");
			for(Document currentPedido : pedidos) {
				System.out.println("---");
				System.out.println("ID Pedido: " + currentPedido.get("idPedido"));
				System.out.println("Fecha: " + currentPedido.get("fecha"));
				System.out.println("Estado:  " + currentPedido.get("estado"));
			}
			System.out.println("------------------------------------------------------------");
	}	
}
	
	public static void Ejercicio_2(MongoCollection<Document> Collection, String productoBuscado) {
	
		FindIterable<Document> resultado = Collection.find(Filters.elemMatch("contiene", Filters.eq("nombre", productoBuscado)));
		
		for(Document CurrentDocument : resultado) {
			System.out.print("-------------------------------------------------------------------------------------------\n");
			System.out.println("ID del Pedido: " + CurrentDocument.get("idPedido"));
			Document currentClient = (Document) CurrentDocument.get("cliente");
			System.out.println("Nombre del Cliente: " + currentClient.get("nombre"));
			List<Document> contiene = (List<Document>) CurrentDocument.get("contiene");
			if(contiene == null) {
				System.out.println("El documento actual no tiene productos!");
			} else {
				for(Document currentProduct: contiene ) {
					if(currentProduct.get("nombre").equals(productoBuscado)) {
						System.out.println("Producto: " + currentProduct.get("nombre") + " (Producto Buscado!)");
					} else {
						System.out.println("Producto: " + currentProduct.get("nombre"));
					}
				}
			}
			System.out.print("-------------------------------------------------------------------------------------------\n");
		}
	}
	
	// Obtener todos los productos almacenados en un depósito específico
	
	public static void Ejercicio_1(MongoCollection<Document> Collection, int idDeposito) {
		
		FindIterable<Document> resultado = Collection.find(Filters.eq("idDeposito",idDeposito));
		
		for(Document CurrentDocument : resultado) {
			System.out.println("Nombre del Deposito: " + CurrentDocument.get("nombre"));
			List<Document> productos = (List<Document>) CurrentDocument.get("productos");
			if(productos == null) {
				System.out.println("El documento actual no tiene productos!");
			} else {
				for(Document currentProduct: productos ) {
					System.out.println("Producto: " + currentProduct.get("nombre"));
				}
			}
		}
	}
	
	public static void agregarDatosEnvios(MongoCollection<Document> Collection) throws ParseException {
		
		SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		// Second List of Products
	    
	    List<Document> productos2 = new ArrayList<>();
	    
	    productos2.add(createProducto(1, "Arroz", 120).append("deposito", "Juanjo Depositos")); // Reused product
	    productos2.add(createProducto(3, "Aceite", 85).append("deposito", "Juanjo Depositos"));  // Reused product
	    productos2.add(createProducto(7, "Pasta", 200).append("deposito", "Fede Depositos"));  // New product
	    productos2.add(createProducto(8, "Lentejas", 150).append("deposito", "Fede Depositos")); // New product
	    productos2.add(createProducto(9, "Sopa Instantánea", 90).append("deposito", "Juanjo Depositos")); // New product
	    productos2.add(createProducto(10, "Cereal", 200).append("deposito", "Juanjo Depositos")); // New product 
	   
	    // Third List of Products
	    
	    List<Document> productos3 = new ArrayList<>();
	    
	    productos3.add(createProducto(11, "Ramen", 120).append("deposito", "Juanjo Depositos")); // Meme product
	    productos3.add(createProducto(12, "Energizante", 85).append("deposito", "Juanjo Depositos"));  // Meme product
	    
	    
	    Document newPedido2 = new Document("idPedido",2)
				.append("cliente", new Document("idCliente", 2).append("nombre", "Lautaro Daniel Yedro"))
				.append("fecha", DateFormat.parse("2023-09-25"))
				.append("estado", "Entregado")
				.append("contiene", productos2)
		;
	    
	    Document newPedido3 = new Document("idPedido",3)
				.append("cliente", new Document("idCliente", 1).append("nombre", "Kenji Uchida Noziglia"))
				.append("fecha", DateFormat.parse("2023-09-24"))
				.append("estado", "Entregado")
				.append("contiene", productos3)
		;

		Document newEnvio = new Document("idEnvio", 1)
		.append("fecha", DateFormat.parse("2023-09-26"))
		.append("Transportista", "Juan Perez")
		.append("estadoEnvio", "Entregado")
		.append("contiene", Arrays.asList(newPedido2, newPedido3))
		;
		
		Collection.insertOne(newEnvio);
	}
	
	
	public static void agregarDatosPedidos(MongoCollection<Document> Collection) throws ParseException {
		
		List<Document> productos = new ArrayList<>();
		
	    productos.add(createProducto(1, "Arroz", 150).append("precio", 500));
	    productos.add(createProducto(2, "Frijoles", 200).append("precio", 2500));
	    productos.add(createProducto(3, "Aceite", 100).append("precio", 1500));
	    productos.add(createProducto(4, "Azúcar", 250).append("precio", 3500));
	    productos.add(createProducto(5, "Sal", 300).append("precio", 1100));
	    productos.add(createProducto(6, "Harina", 180).append("precio", 2500));
		
	    // Second List of Products
	    
	    List<Document> productos2 = new ArrayList<>();
	    
        productos2.add(createProducto(1, "Arroz", 120).append("precio", 2000)); // Reused product
        productos2.add(createProducto(3, "Aceite", 85).append("precio", 2500));  // Reused product
        productos2.add(createProducto(7, "Pasta", 200).append("precio", 2120));  // New product
        productos2.add(createProducto(8, "Lentejas", 150).append("precio", 1500)); // New product
        productos2.add(createProducto(9, "Sopa Instantánea", 90).append("precio", 3000)); // New product
        productos2.add(createProducto(10, "Cereal", 200).append("precio", 5110)); // New product 
	    
	    
		SimpleDateFormat DateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		Document newPedido = new Document("idPedido",1)
				.append("cliente", new Document("idCliente", 1).append("nombre", "Kenji Uchida Noziglia"))
				.append("fecha", DateFormat.parse("2024-11-10"))
				.append("estado", "Pendiente")
				.append("contiene", productos)
		;
		
		Document newPedido2 = new Document("idPedido",2)
				.append("cliente", new Document("idCliente", 2).append("nombre", "Lautaro Daniel Yedro"))
				.append("fecha", DateFormat.parse("2023-09-25"))
				.append("estado", "Entregado")
				.append("contiene", productos2)
		;
		
		Collection.insertMany(Arrays.asList(newPedido, newPedido2));
	}
	
	public static void agregarDatos(MongoCollection<Document> Collection) {
	// i create some products for the Depot
		
		List<Document> productos = new ArrayList<>();
		
	    productos.add(createProducto(1, "Arroz", 150));
	    productos.add(createProducto(2, "Frijoles", 200));
	    productos.add(createProducto(3, "Aceite", 100));
	    productos.add(createProducto(4, "Azúcar", 250));
	    productos.add(createProducto(5, "Sal", 300));
	    productos.add(createProducto(6, "Harina", 180));

	    // Second Array of Products
	    
	    List<Document> productos2 = new ArrayList<>();
	    
        productos2.add(createProducto(1, "Arroz", 120)); // Reused product
        productos2.add(createProducto(3, "Aceite", 85));  // Reused product
        productos2.add(createProducto(7, "Pasta", 200));  // New product
        productos2.add(createProducto(8, "Lentejas", 150)); // New product
        productos2.add(createProducto(9, "Sopa Instantánea", 90)); // New product
        productos2.add(createProducto(10, "Cereal", 200)); // New product 
	    
	    // I create a new Deposito
	    
	    Document Deposito_A = crearDeposito(1, "Juanjo Depositos", "Av.Corrientes 2500", "500", productos);
	    
	    Document Deposito_B = crearDeposito(2, "Fede Depositos", "Av.Uruguay 1335", "250", productos2);
	    
	    Collection.insertMany(Arrays.asList(Deposito_A, Deposito_B));
	}
	
	public static Document createProducto(int idProducto, String nombre, int stock) {
		    return new Document("idProducto", idProducto)
		            .append("nombre", nombre)
		            .append("stock", stock);
		}

	public static Document crearDeposito(int idDeposito, String nombre, String ubicacion, String capacidad, List<Document> productos) {
		Document newDeposito = new Document("idDeposito", idDeposito)
	            .append("nombre", nombre)
	            .append("ubicacion", ubicacion)
	            .append("capacidad", capacidad)
	            .append("productos", productos);
		return newDeposito;
	}
	
	public static MongoCollection<Document> connectToMongoDB(String database, String collectionName) {
		String url = "mongodb://localhost:32769";
		try {
			MongoClient mongoClient = MongoClients.create(url);
			// I retrieve the collection
			MongoCollection<Document> collection = mongoClient.getDatabase(database).getCollection(collectionName);
			System.out.println("Conectado a MongoDB");
			return collection;
		} catch (Exception var3) {
			throw new Error("No se pudo conectar a MongoDB");
		}
	}
}
