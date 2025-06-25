from pymongo import MongoClient

MONGO_URI = ""
DB_NAME = ""

def query1(db: object, state: str, year: int) -> list:
	"""Porcentagem da emissão da agropecuária sobre emissão total em um estado em determinado ano"""

	pipeline = [
		{ "$match": { "ano_em": year } },

		{
			"$lookup": {
				"from": "estado",
				"localField": "localizacao.id_uf",
				"foreignField": "_id",
				"as": "estado"
			}
		},

		{ "$unwind": "$estado" },

		{ "$match": { "estado.nome_uf": state } },

		{
			"$group": {
				"_id": None,
				"total_emissao": {"$sum": "$qtd_em"},
				"agro_emissao": {
					"$sum": {
						"$cond": [
							{"$eq": ["$origem.setor_origem", "Agropecuária"]},
							"$qtd_em",
							0
						]
					}
				}
			}
		},

		{
			"$project": {
				"_id": 0,
				"estado": state,
				"ano": {"$literal": year},
				"emitido_agro": "$agro_emissao",
				"porcentagem_sobre_total": {
					"$cond": [
						{"$gt": ["$total_emissao", 0]},
						{"$multiply": [{"$divide": ["$agro_emissao", "$total_emissao"]}, 100]},
						None
					]
				}
			}
		}]

	return list(db.emissao.aggregate(pipeline))

def query2(db: object) -> list:
	"""Evolução da (emissão agropecuária/área rural) e (emissão indústria/área urbana) ao longo dos anos"""

	pipeline = [
		{ "$unwind": "$municipios" },

		{ "$unwind": "$municipios.areas" },

		{
			"$group": {
				"_id": "$municipios.areas.tipo_area",
				"area_rural": {
					"$sum": {
						"$cond": [
							{"$eq": ["$municipios.areas.tipo_area", "RURAL"]},
							"$municipios.areas.tam_area",
							0
						]
					}
				},
				"area_total": {"$sum": "$municipios.area_total"}
			}
		},

		{
			"$group": {
				"_id": None,
				"area_rural": {"$sum": "$area_rural"},
				"area_total": {"$first": "$area_total"}
			}
		},

		{
			"$project": {
				"area_rural": 1,
				"area_urbana": {"$subtract": ["$area_total", "$area_rural"]}
			}
		},

		{
			"$lookup": {
				"from": "emissao",
				"pipeline": [
					{
						"$match": { "origem.setor_origem": {"$in": ["Processos Industriais", "Agropecuária"]} }
					},
					{
						"$group": {
							"_id": {
								"ano": "$ano_em",
								"setor": "$origem.setor_origem"
							},
							"emissao": {"$sum": "$qtd_em"}
						}
					},
					{"$sort": {"_id.ano": 1}}
				],
				"as": "emissoes"
			}
		},

		{ "$unwind": "$emissoes" },

		{
			"$project": {
				"ano": "$emissoes._id.ano",
				"setor": "$emissoes._id.setor",
				"emissao": "$emissoes.emissao",
				"area_rural": 1,
				"area_urbana": 1
			}
		},

		{
			"$group": {
				"_id": "$ano",
				"emissao_por_area_rural": {
					"$sum": {
						"$cond": [
							{"$eq": ["$setor", "Agropecuária"]},
							{"$cond": [{"$ne": ["$area_rural", 0]}, {"$divide": ["$emissao", "$area_rural"]}, 0]},
							0
						]
					}
				},
				"emissao_por_area_urbana": {
					"$sum": {
						"$cond": [
							{"$eq": ["$setor", "Processos Industriais"]},
							{"$cond": [{"$ne": ["$area_urbana", 0]}, {"$divide": ["$emissao", "$area_urbana"]}, 0]},
							0
						]
					}
				}
			}
		},

		{
			"$project": {
				"_id": 0,
				"ano": "$_id",
				"emissao_por_area_rural": 1,
				"emissao_por_area_urbana": 1,
				"proporcao_urb_rur": {
					"$cond": [
						{"$ne": ["$emissao_por_area_rural", 0]},
						{"$divide": ["$emissao_por_area_urbana", "$emissao_por_area_rural"]},
						0
					]
				}
			}
		},

		{"$sort": {"ano": 1}}]

	return list(db.estado.aggregate(pipeline))

def query3(db: object, state: str) -> list:
	"""Aumento da emissão de gases em São Paulo e nos outros estados no período de 1970 - 2023 e comparação entre aumento de SP e média dos outros estados
[aumento relativo mostra a intensidade do crescimento de São Paulo em relação ao resto do Brasil]"""

	pipeline = [
		{ "$match": { "ano_em": {"$in": [1970, 2023]} } },

		{
			"$lookup": {
				"from": "estado",
				"localField": "localizacao.id_uf",
				"foreignField": "_id",
				"as": "estado"
			}
		},

		{ "$unwind": "$estado" },

		{
			"$group": {
				"_id": "$ano_em",
				"emissoes_state": {
					"$sum": {
						"$cond": [
							{"$eq": ["$estado.nome_uf", state]},
							"$qtd_em",
							0
						]
					}
				},

				"emissoes_outros": {
					"$sum": {
						"$cond": [
							{"$ne": ["$estado.nome_uf", state]},
							"$qtd_em",
							0
						]
					}
				}
			}
		},

		{
			"$project": {
				"ano": "$_id",
				"emissoes_state": 1,
				"media_outros": {"$divide": ["$emissoes_outros", 26]},
				"_id": 0
			}
		},

		{
			"$group": {
				"_id": None,
				"anos": {
					"$push": {
						"ano": "$ano",
						"emissoes_state": "$emissoes_state",
						"media_outros": "$media_outros"
					}
				}
			}
		},

		{
			"$project": {
				"aumento_state": {
					"$divide": [
						{
							"$arrayElemAt": ["$anos.emissoes_state", {"$indexOfArray": ["$anos.ano", 2023]}]
						},
						{
							"$arrayElemAt": ["$anos.emissoes_state", {"$indexOfArray": ["$anos.ano", 1970]}]
						}
					]
				},
				"aumento_medio": {
					"$divide": [
						{
							"$arrayElemAt": ["$anos.media_outros", {"$indexOfArray": ["$anos.ano", 2023]}]
						},
						{
							"$arrayElemAt": ["$anos.media_outros", {"$indexOfArray": ["$anos.ano", 1970]}]
						}
					]
				}
			}
		},

		{
			"$project": {
				"_id": 0,
				"aumento_state": 1,
				"aumento_medio": 1,
				"aumento_relativo": {
					"$cond": [
						{"$ne": ["$aumento_medio", 0]},
						{"$divide": ["$aumento_state", "$aumento_medio"]},
						None
					]
				}
			}
		}]

	return list(db.emissao.aggregate(pipeline))

def query4(db: object, region: str, year: int) -> list:
	"""Porcentagem de emissao dos top 5 produtos mais emissores na agropecuária em uma determinada região e ano"""

	pipeline = [
		{
			"$lookup": {
				"from": "estado",
				"localField": "localizacao.id_uf",
				"foreignField": "_id",
				"as": "estado"
			}
		},

		{ "$unwind": "$estado" },

		{
			"$match": {
				"ano_em": year,
				"origem.tipo_origem": "Emissão",
				"origem.setor_origem": "Agropecuária",
				"estado.regiao.nome_reg": region
			}
		},

		{
			"$group": {
				"_id": "$produto.nome_produto",
				"qtd_em": { "$sum": "$qtd_em" }
			}
		},

		{
			"$group": {
				"_id": None,
				"produtos": {
					"$push": {
						"produto": "$_id",
						"qtd_em": "$qtd_em"
					}
				},
				"total_em": { "$sum": "$qtd_em" }
			}
		},

		{ "$unwind": "$produtos" },

		{
			"$project": {
				"_id": 0,
				"produto": "$produtos.produto",
				"qtd_em": "$produtos.qtd_em",
				"porcentagem": {
					"$round": [
						{ "$multiply": [{ "$divide": ["$produtos.qtd_em", "$total_em"] }, 100] },
						2
					]
				}
			}
		},

		{ "$sort": { "porcentagem": -1 } },

		{ "$limit": 5 }]

	return list(db.emissao.aggregate(pipeline))

def query5(db: object) -> list:
	"""Top 10 anos com maior balanço qtd_em + qtd_rem no século 21"""

	pipeline = [
		{
			"$match": {
				"ano_em": { "$gte": 2001 },
				"gas.caracteristica_gas": { "$regex": "GWP", "$options": "i" }
			}
		},

		{
			"$group": {
				"_id": { "ano": "$ano_em" },
				"qtd_em": {
					"$sum": {
						"$cond": [
							{ "$eq": ["$origem.tipo_origem", "Emissão"] },
							"$qtd_em",
							0
						]
					}
				},

				"qtd_rem": {
					"$sum": {
						"$cond": [
							{ "$eq": ["$origem.tipo_origem", "Remoção"] },
							"$qtd_em",
							0
						]
					}
				}
			}
		},

		{
			"$project": {
				"_id": 0,
				"ano": "$_id.ano",
				"nome_gas": "$_id.nome_gas",
				"qtd_em": 1,
				"qtd_rem": 1,
				"qtd_em_liquida": { "$add": ["$qtd_em", "$qtd_rem"] }
			}
		},

		{ "$sort": { "qtd_em_liquida": -1 } },

		{ "$limit": 10 }]

	return list(db.emissao.aggregate(pipeline))

def main():
	client = MongoClient(MONGO_URI)
	db = client[DB_NAME]

	print("\n========================== Query1 ==========================")
	print(query1(db, "MATO GROSSO", 2023))

	print("\n========================== Query2 ==========================")
	print(query2(db))

	print("\n========================== Query3 ==========================")
	print(query3(db, "SÃO PAULO"))

	print("\n========================== Query4 ==========================")
	print(query4(db, "SUDESTE", 2005))

	print("\n========================== Query5 ==========================")
	print(query5(db))

	client.close()

main()