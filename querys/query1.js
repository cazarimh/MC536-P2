db.emissao.aggregate([
  {
    $match: {
      ano_em: ano
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
      "estado.nome_uf": "estado_nome"
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
      estado: "estado_nome",
      ano: ano,
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
