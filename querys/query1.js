/*
Porcentagem da emissão da agropecuária sobre emissão total em um estado em determinado ano 
*/

use('MC536-P2');

db.emissao.aggregate([
  {
    $match: {
      ano_em: 2023
    }
  },

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
      "estado.nome_uf": 'MATO GROSSO'
    }
  },

  {
    $group: {
      _id: null,
      total_emissao: { $sum: "$qtd_em" },
      agro_emissao: {
        $sum: {
          $cond: [
            { $eq: ["$origem.setor_origem", "Agropecuária"] },
            "$qtd_em",
            0
          ]
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      estado: 'MATO GROSSO',
      ano: {$literal: 2023},
      emitido_agro: "$agro_emissao",
      porcentagem_sobre_total: {
        $cond: [
          { $gt: ["$total_emissao", 0] },
          { $multiply: [{ $divide: ["$agro_emissao", "$total_emissao"] }, 100] },
          null
        ]
      }
    }
  }
])
