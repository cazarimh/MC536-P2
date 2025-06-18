const database = 'MC536-P2';
const emissao = 'emissao';
const estado = 'estado';
const bioma = 'bioma';

use(database);

db.createCollection(emissao, {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["ano_em", "qtd_em", "localizacao", "origem", "produto", "gas"],
            properties: {
                ano_em: {
                    bsonType: "int",
                    description: "Ano registrado da emissão, é obrigatório e deve ser um ano entre 1970 e 2023."
                },
                qtd_em: {
                    bsonType: ["double", "int"],
                    description: "Quantidade registrada da emissão em toneladas, é obrigatória e deve ser um número."
                },
                localizacao: {
                    bsonType: "object",
                    properties: {
                        id_uf: {
                            bsonType: "objectId",
                            description: "Referência ao estado em que a emissão foi registrada."
                        },
                        id_bio: {
                            bsonType: "objectId",
                            description: "Referência ao bioma em que a emissão foi registrada."
                        }
                    }
                },
                origem: {
                    bsonType: "object",
                    required: ["tipo_origem", "setor_origem", "categoria_origem", "subcategoria_origem"],
                    properties: {
                        tipo_origem: {
                            bsonType: "string",
                            enum: ["Emissão", "Emissão NCI", "Remoção", "Remoção NCI", "Bunker"],
                            description: "Tipo da origem, é obrigatório e deve ser uma string (restrito a Emissão, Emissão NCI, Remoção, Remoção NCI, Bunker)."
                        },
                        setor_origem: {
                            bsonType: "string",
                            description: "Setor de origem, é obrigatório e deve ser uma string."
                        },
                        categoria_origem: {
                            bsonType: "string",
                            description: "Categoria de origem, é obrigatório e deve ser uma string."
                        },
                        subcategoria_origem: {
                            bsonType: "string",
                            description: "Subcategoria de origem, é obrigatório e deve ser uma string."
                        }
                    }
                },
                produto: {
                    bsonType: "object",
                    required: ["nome_produto", "detalhamento_produto", "recorte_produto", "atvgeral_produto"],
                    properties: {
                        nome_produto: {
                            bsonType: "string",
                            description: "Nome do produto, é obrigatório e deve ser uma string."
                        },
                        detalhamento_produto: {
                            bsonType: "string",
                            description: "Detalhamento do produto, é obrigatório e deve ser uma string."
                        },
                        recorte_produto: {
                            bsonType: "string",
                            description: "Recorte do produto, é obrigatório e deve ser uma string."
                        },
                        atvgeral_produto: {
                            bsonType: "string",
                            description: "Atividade Geral do produto, é obrigatório e deve ser uma string."
                        }
                    }
                },
                gas: {
                    bsonType: "object",
                    required: ["nome_gas"],
                    properties: {
                        nome_gas: {
                            bsonType: "string",
                            description: "Nome do gas, é obrigatório e deve ser uma string."
                        },
                        caracteristica_gas: {
                            bsonType: "string",
                            description: "Caracteristica do gas, deve ser uma string."
                        }
                    }
                }
            }
        }
    },
    validationAction: "error",
    validationLevel: "strict"
});

db.createCollection(estado, {
    validator: {
        $jsonSchema: {
        bsonType: "object",
        required: ["cod_uf", "nome_uf", "uf", "regiao", "municipios"],
        properties: {
            cod_uf: {
                bsonType: "int",
                minimum: 10,
                maximum: 99,
                description: "Código da UF do IBGE, é obrigatório e deve ser um inteiro positivo de 2 dígitos."
            },
            nome_uf: {
                bsonType: "string",
                description: "Nome do estado, é obrigatório e deve ser uma string."
            },
            uf: {
                bsonType: "string",
                pattern: "^[A-Z]{2}$",
                description: "Sigla do estado (UF), é obrigatória e deve ter 2 caracteres maiúsculos."
            },
            regiao: {
                bsonType: "object",
                required: ["cod_reg", "nome_reg"],
                properties: {
                    cod_reg: {
                        bsonType: "int",
                        minimum: 0,
                        maximum: 9,
                        description: "Código da região, é obrigatório e deve ser um inteiro positivo de 1 dígito."
                    },
                    nome_reg: {
                        bsonType: "string",
                        description: "Nome da região, é obrigatório e deve ser uma string."
                    }
                }
            },
            municipios: {
                bsonType: "array",
                items: { 
                    bsonType: "object",
                    required: ["cod_mu", "nome_mu", "area_total", "num_imoveis", "microrregiao", "id_bio", "areas"],
                    properties: {
                        cod_mu: {
                            bsonType: "int",
                            minimum: 1000000,
                            maximum: 9999999,
                            description: "Código do município do IBGE, é obrigatório e deve ser um inteiro positivo de 7 dígitos."
                        },
                        nome_mu: {
                            bsonType: "string",
                            description: "Nome do município, é obrigatório e deve ser uma string."
                        },
                        area_total: {
                            bsonType: ["double", "int"],
                            description: "Área total do município, é obrigatória e deve ser um número positivo."
                        },
                        num_imoveis: {
                            bsonType: "int",
                            description: "Quantidade de imóveis rurais no município, é obrigatório e deve ser um inteiro positivo."
                        },
                        microrregiao: {
                            bsonType: "object",
                            required: ["cod_mi", "nome_mi"],
                            properties: {
                                cod_mi: {
                                    bsonType: "int",
                                    minimum: 10000,
                                    maximum: 99999,
                                    description: "Código da microrregião do IBGE, é obrigatório e deve ser um inteiro positivo de 5 dígitos."
                                },
                                nome_mi: {
                                    bsonType: "string",
                                    description: "Nome da microrregião, é obrigatório e deve ser uma string."
                                }
                            }
                        },
                        id_bio: {
                            bsonType: "objectId",
                            description: "Referência ao bioma do município."
                        },
                        areas: {
                            bsonType: "array",
                            items: {
                                bsonType: "object",
                                required: ["tipo_area", "tam_area"],
                                properties: {
                                    tipo_area: {
                                        bsonType: "string",
                                        enum: ["RURAL", "PROT", "PRESERV", "PROPRE", "EXP"],
                                        description: "Tipo da área, é obrigatório e deve ser uma string (restrito a RURAL, PROT, PRESERV, PROPRE, EXP)."
                                    },
                                    tam_area: {
                                        bsonType: ["double", "int"],
                                        description: "Tamanho da área, é obrigatória e deve ser um número positivo."
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        }
    },
    validationAction: "error",
    validationLevel: "strict"
});

db.createCollection(bioma, {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["nome_bio"],
            properties: {
                nome_bio: {
                    bsonType: "string",
                    description: "Nome do bioma, é obrigatório e deve ser uma string."
                }
            }
        }
    }
})