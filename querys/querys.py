from pymongo import MongoClient

def query1(db, estado_nome, ano):
    # Porcentagem da emissão da agropecuária sobre emissão total em um estado em determinado ano

    # identifica estado a partir do nome
    estado = db.estado.find_one({"nome_uf": estado_nome})

    # verifica se o estado existe
    if not estado:
        return []

    id_uf = estado["_id"]

    # filtra emissão total para o estado e agrupa as emissões ao longo do ano
    total = db.emissao.aggregate([
        {"$match": {"ano_em": ano, "localizacao.id_uf": id_uf}},
        {"$group": {"_id": None, "total_emissao": {"$sum": "$qtd_em"}}}
    ])

    # filtra a emissão do agro para o estado e agrupa as emissões ao longo do ano
    agro = db.emissao.aggregate([
        {"$match": {"ano_em": ano, "localizacao.id_uf": id_uf, "origem.setor_origem": "Agropecuária"}},
        {"$group": {"_id": None, "emitido_agro": {"$sum": "$qtd_em"}}}
    ])

    # converte resultados para listas
    total_result = list(total)
    agro_result = list(agro)

    # se os resultados não estiverem vazios:
    if total_result and agro_result:
        total_emissao = total_result[0]["total_emissao"] # total de emissões no estado no ano
        emitido_agro = agro_result[0]["emitido_agro"]    # total de emissões do agro no estado no ano
        porcentagem = (emitido_agro * 100.0) / total_emissao if total_emissao != 0 else 0

        return [{
            "ano": ano,
            "estado": estado_nome,
            "emitido_agro": emitido_agro,
            "porcentagem_sobre_total": porcentagem
        }]

    return []


def query2(db):
    # Evolução da (emissão agropecuária/área rural) e (emissão indústria/área urbana) ao longo dos anos

    # pipeline para cálculo da área total e da área rural
    pipeline_area = [
        # cria documentos para cada município
        {"$unwind": "$municipios"},

        # cria documentos para cada área de cada município
        {"$unwind": "$municipios.areas"},

        # filtra cada área do município como rural e faz o agrupamento de todas áreas rurais
        {"$match": {"municipios.areas.tipo_area": "RURAL"}},
        {"$group": {
            "_id": None,
            "area_rural": {"$sum": "$municipios.areas.tam_area"},
            "area_total": {"$sum": "$municipios.area_total"}
        }},
        {"$project": {
            "area_rural": 1,
            "area_urbana": {"$subtract": ["$area_total", "$area_rural"]}
        }}
    ]

    # executa o pipeline para calcular as áreas
    areas = list(db.estado.aggregate(pipeline_area))
    if not areas:
        return []

    area_rural = areas[0]["area_rural"]
    area_urbana = areas[0]["area_urbana"]

    # pipeline para agrupar as emissões por setor e ano
    pipeline_emissao = [
        {"$match": {"origem.setor_origem": {"$in": ["Processos Industriais", "Agropecuária"]}}},
        {"$group": {
            "_id": {"ano": "$ano_em", "setor": "$origem.setor_origem"},
            "emissao": {"$sum": "$qtd_em"}
        }}
    ]

    # executa a pipeline das emissões
    emissoes = list(db.emissao.aggregate(pipeline_emissao))

    # dicionário para organizar resultados por ano
    resultado_por_ano = {}

    for emissao in emissoes:
        ano = emissao["_id"]["ano"]
        setor = emissao["_id"]["setor"]
        qtd_emissao = emissao["emissao"]

        # se o ano não estiver, cria pela primeira vez
        if ano not in resultado_por_ano:
            resultado_por_ano[ano] = {"ano": ano, "emissao_por_area_rural": 0, "emissao_por_area_urbana": 0}

        # calcula a emissão por área específica de acordo com o setor
        if setor == "Agropecuária":
            resultado_por_ano[ano]["emissao_por_area_rural"] = qtd_emissao / area_rural if area_rural != 0 else 0
        elif setor == "Processos Industriais":
            resultado_por_ano[ano]["emissao_por_area_urbana"] = qtd_emissao / area_urbana if area_urbana != 0 else 0

    # resultado em uma lista ordenada por ano 
    resultado = []
    for dados in sorted(resultado_por_ano.values(), key=lambda x: x["ano"]):
        rural = dados["emissao_por_area_rural"]
        urbana = dados["emissao_por_area_urbana"]
        proporcao = urbana / rural if rural != 0 else 0

        resultado.append({
            "ano": dados["ano"],
            "emissao_por_area_rural": rural,
            "emissao_por_area_urbana": urbana,
            "proporcao_urb_rur": proporcao
        })

    return resultado

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client[""]

    print("\n========================== Query1 ==========================")
    print(query1(db, "SÃO PAULO", 2022))

    print("\n========================== Query2 ==========================")
    print(query2(db))

    client.close()

if __name__ == "__main__":
    main()
