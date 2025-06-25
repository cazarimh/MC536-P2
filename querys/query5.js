/*
Top 10 anos com maior balanço qtd_em + qtd_rem no século 21
*/

use('MC536');

db.emissao.aggregate([
    {
        $match: {
            ano_em: { $gte: 2001 },
            "gas.caracteristica_gas": { $regex: "GWP", $options: "i" }
        }
    },

    {
        $group: {
            _id: {
                ano: "$ano_em"
        },
        qtd_em: {
            $sum: {
                $cond: [{ $eq: ["$origem.tipo_origem", "Emissão"] }, "$qtd_em", 0]
            }
        },
        qtd_rem: {
            $sum: {
                $cond: [{ $eq: ["$origem.tipo_origem", "Remoção"] }, "$qtd_em", 0]
            }
        }
        }
    },

    {
        $project: {
            _id: 0,
            ano: "$_id.ano",
            nome_gas: "$_id.nome_gas",
            qtd_em: 1,
            qtd_rem: 1,
            qtd_em_liquida: { $add: ["$qtd_em", "$qtd_rem"] }
        }
    },

    { $sort: { qtd_em_liquida: -1 } },

    { $limit: 10 }
])