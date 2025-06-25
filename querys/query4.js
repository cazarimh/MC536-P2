/*
Porcentagem de emissao dos top 5 produtos mais emissores na agropecuária
*/

use('MC536');

db.emissao.aggregate([
    {
        $lookup: {
            from: "estado",
            localField: "localizacao.id_uf",
            foreignField: "_id",
            as: "estado"
        }
    },

    { $unwind: "$estado" },
    
    {
        $match: {
            "ano_em": 2005,
            "origem.tipo_origem": "Emissão",
            "origem.setor_origem": "Agropecuária",
            "estado.regiao.nome_reg": "SUDESTE"
        }
    },

    {
        $group: {
            "_id": "$produto.nome_produto",
            "qtd_em": { $sum: "$qtd_em" },
        }
    },

    {
        $group: {
            "_id": null,
            "produtos": {
                $push: {
                    "produto": "$_id",
                    "qtd_em": "$qtd_em"
                }
            },
            "total_em": { $sum: "$qtd_em" }
        }
    },

    { $unwind: "$produtos" },

    {
        $project: {
            "_id": 0,
            "produto": "$produtos.produto",
            "qtd_em": "$produtos.qtd_em",
            "porcentagem": {
                $round: [
                    {$multiply: [{$divide: ["$produtos.qtd_em", "$total_em"]}, 100]},
                    2
                ]
            }
        }
    },

    { $sort: { "porcentagem": -1} },
    { $limit: 5 }
])
   
