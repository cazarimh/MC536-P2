from pymongo import MongoClient
import pandas as pd

MONGO_URL = ""
DB_NAME = ""

def main():
    client = MongoClient(MONGO_URL)
    database = client[DB_NAME]

    embrapa_dataset = pd.read_csv("./dataset/cidadesPreserv.csv")

    bioma = embrapa_dataset['mu_bio'].drop_duplicates()
    bioma_data = list(map(lambda x: {"nome_bio": x.upper()}, bioma))

    database["bioma"].delete_many({})
    database["bioma"].insert_many(bioma_data)

    result = database["bioma"].find({}, {"nome_bio": 1, "_id": 1}).to_list()
    bioma_dict = {bioma["nome_bio"]: bioma["_id"] for bioma in result}

    estado = embrapa_dataset[['cd_geocuf_', 'nm_estado_', 'sigla', 'nm_regiao']].drop_duplicates()
    estado = list(zip(estado['cd_geocuf_'], estado['nm_estado_'], estado['sigla'], list(map(lambda x: x//10, estado["cd_geocuf_"])), estado["nm_regiao"]))

    estado_data = []
    estado_map = {}
    for i in range(len(estado)):
        estado_data.append({"cod_uf": estado[i][0], "nome_uf": estado[i][1], "uf": estado[i][2], "regiao": {"cod_reg": estado[i][3], "nome_reg": estado[i][4]}, "municipios": []})
        estado_map[estado[i][0]] = i
    
    municipios = embrapa_dataset[['cd_mun', 'nm_municip', 'munic_ha', 'n_imrur', 'cd_geocmi', 'nm_micro', 'mu_bio', 'imrur_ha', 'uctia_ha', 'adpv_ha', 'propre_ha', 'im_di_ha']]
    municipios = list(zip(municipios['cd_mun'], municipios['nm_municip'], list(map(lambda x: float(x)/1000000, municipios['munic_ha'])), municipios['n_imrur'],
                          municipios['cd_geocmi'], municipios['nm_micro'],
                          list(map(lambda x: bioma_dict[x.upper()], municipios['mu_bio'])),
                          list(map(lambda x: float(x)/1000000, municipios['imrur_ha'])),
                          list(map(lambda x: float(x)/1000000, municipios['uctia_ha'])),
                          list(map(lambda x: float(x)/1000000, municipios['adpv_ha'])),
                          list(map(lambda x: float(x)/1000000, municipios['propre_ha'])),
                          list(map(lambda x: float(x)/1000000, municipios['im_di_ha']))))
    
    for i in municipios:
        municipio_atual = {"cod_mu": i[0], "nome_mu": i[1], "area_total": i[2], "num_imoveis": i[3],
                           "microrregiao": {"cod_mi": i[4], "nome_mi": i[5]},
                           "id_bio": i[6],
                           "areas": [{"tipo_area": "RURAL", "tam_area": i[7]},
                                     {"tipo_area": "PROT", "tam_area": i[8]},
                                     {"tipo_area": "PRESERV", "tam_area": i[9]},
                                     {"tipo_area": "PROPRE", "tam_area": i[10]},
                                     {"tipo_area": "EXP", "tam_area": i[11]}]}
        
        estado_data[estado_map[i[0]//100000]]["municipios"].append(municipio_atual)
    
    database["estado"].delete_many({})
    database["estado"].insert_many(estado_data)

    result = database["estado"].find({}, {"nome_uf": 1, "_id": 1}).to_list()
    estado_dict = {estado["nome_uf"]: estado["_id"] for estado in result}

    database["emissao"].delete_many({})

    seeg_dataset = pd.read_csv("./dataset/gasesEE-medicoes_C1.csv", keep_default_na=False)

    for i, row in seeg_dataset.iterrows():
        emissao_data = []

        uf_bio = {}
        if (row['Estado'] != "Não Alocado"):
            uf_bio["id_uf"] = estado_dict[row['Estado'].upper()]
        if (row['Bioma'] != "NA"):
            uf_bio["id_bio"] = bioma_dict[row['Bioma'].upper()]
        
        if (row['Gás'][:4] == "CO2e"):
            gas_atual = {"nome_gas": "CO2e", "caracteristica_gas": row['Gás'][4:]}
        else:
            gas_atual = {"nome_gas": row['Gás']}

        for ano in range(1970, 2024):
            
            emissao_atual = {"ano_em": ano, "qtd_em": float(row[str(ano)].replace(',', '.')),
                             "localizacao": uf_bio,
                             "origem": {"tipo_origem": row['Emissão/Remoção/Bunker'], "setor_origem": row['Setor de emissão'], "categoria_origem": row['Categoria emissora'], "subcategoria_origem": row['Sub-categoria emissora']},
                             "produto": {"nome_produto": row['Produto ou sistema'], "detalhamento_produto": row['Detalhamento'], "recorte_produto": row['Recorte'], "atvgeral_produto": row['Atividade geral']},
                             "gas": gas_atual}
            
            emissao_data.append(emissao_atual)
        
        database["emissao"].insert_many(emissao_data)
        print(f'Linha {i} da parte 1/4, inserida com sucesso!')
    
    seeg_dataset = pd.read_csv("./dataset/gasesEE-medicoes_C2.csv", keep_default_na=False)

    for i, row in seeg_dataset.iterrows():
        emissao_data = []

        uf_bio = {}
        if (row['Estado'] != "Não Alocado"):
            uf_bio["id_uf"] = estado_dict[row['Estado'].upper()]
        if (row['Bioma'] != "NA"):
            uf_bio["id_bio"] = bioma_dict[row['Bioma'].upper()]
        
        if (row['Gás'][:4] == "CO2e"):
            gas_atual = {"nome_gas": "CO2e", "caracteristica_gas": row['Gás'][4:]}
        else:
            gas_atual = {"nome_gas": row['Gás']}

        for ano in range(1970, 2024):
            
            emissao_atual = {"ano_em": ano, "qtd_em": float(row[str(ano)].replace(',', '.')),
                             "localizacao": uf_bio,
                             "origem": {"tipo_origem": row['Emissão/Remoção/Bunker'], "setor_origem": row['Setor de emissão'], "categoria_origem": row['Categoria emissora'], "subcategoria_origem": row['Sub-categoria emissora']},
                             "produto": {"nome_produto": row['Produto ou sistema'], "detalhamento_produto": row['Detalhamento'], "recorte_produto": row['Recorte'], "atvgeral_produto": row['Atividade geral']},
                             "gas": gas_atual}
            
            emissao_data.append(emissao_atual)
        
        database["emissao"].insert_many(emissao_data)
        print(f'Linha {i} da parte 2/4, inserida com sucesso!')
    
    seeg_dataset = pd.read_csv("./dataset/gasesEE-medicoes_C3.csv", keep_default_na=False)

    for i, row in seeg_dataset.iterrows():
        emissao_data = []

        uf_bio = {}
        if (row['Estado'] != "Não Alocado"):
            uf_bio["id_uf"] = estado_dict[row['Estado'].upper()]
        if (row['Bioma'] != "NA"):
            uf_bio["id_bio"] = bioma_dict[row['Bioma'].upper()]
        
        if (row['Gás'][:4] == "CO2e"):
            gas_atual = {"nome_gas": "CO2e", "caracteristica_gas": row['Gás'][4:]}
        else:
            gas_atual = {"nome_gas": row['Gás']}

        for ano in range(1970, 2024):
            
            emissao_atual = {"ano_em": ano, "qtd_em": float(row[str(ano)].replace(',', '.')),
                             "localizacao": uf_bio,
                             "origem": {"tipo_origem": row['Emissão/Remoção/Bunker'], "setor_origem": row['Setor de emissão'], "categoria_origem": row['Categoria emissora'], "subcategoria_origem": row['Sub-categoria emissora']},
                             "produto": {"nome_produto": row['Produto ou sistema'], "detalhamento_produto": row['Detalhamento'], "recorte_produto": row['Recorte'], "atvgeral_produto": row['Atividade geral']},
                             "gas": gas_atual}
            
            emissao_data.append(emissao_atual)
        
        database["emissao"].insert_many(emissao_data)
        print(f'Linha {i} da parte 3/4, inserida com sucesso!')
    
    seeg_dataset = pd.read_csv("./dataset/gasesEE-medicoes_C4.csv", keep_default_na=False)

    for i, row in seeg_dataset.iterrows():
        emissao_data = []

        uf_bio = {}
        if (row['Estado'] != "Não Alocado"):
            uf_bio["id_uf"] = estado_dict[row['Estado'].upper()]
        if (row['Bioma'] != "NA"):
            uf_bio["id_bio"] = bioma_dict[row['Bioma'].upper()]
        
        if (row['Gás'][:4] == "CO2e"):
            gas_atual = {"nome_gas": "CO2e", "caracteristica_gas": row['Gás'][4:]}
        else:
            gas_atual = {"nome_gas": row['Gás']}

        for ano in range(1970, 2024):
            
            emissao_atual = {"ano_em": ano, "qtd_em": float(row[str(ano)].replace(',', '.')),
                             "localizacao": uf_bio,
                             "origem": {"tipo_origem": row['Emissão/Remoção/Bunker'], "setor_origem": row['Setor de emissão'], "categoria_origem": row['Categoria emissora'], "subcategoria_origem": row['Sub-categoria emissora']},
                             "produto": {"nome_produto": row['Produto ou sistema'], "detalhamento_produto": row['Detalhamento'], "recorte_produto": row['Recorte'], "atvgeral_produto": row['Atividade geral']},
                             "gas": gas_atual}
            
            emissao_data.append(emissao_atual)
        
        database["emissao"].insert_many(emissao_data)
        print(f'Linha {i} da parte 4/4, inserida com sucesso!')

    client.close()

    return 0

main()