db.emissao.aggregate([
  {
    $match: {
      ano_em: { $in: [1970, 2023] }
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
    $group: {
      _id: "$ano_em",
      emissoes_sp: {
        $sum: {
          $cond: [
            { $eq: ["$estado.nome_uf", "SÃO PAULO"] },
            "$qtd_em",
            0
          ]
        }
      },
      emissoes_outros: {
        $sum: {
          $cond: [
            { $ne: ["$estado.nome_uf", "SÃO PAULO"] },
            "$qtd_em",
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      ano: "$_id",
      emissoes_sp: 1,
      media_outros: { $divide: ["$emissoes_outros", 26] },
      _id: 0
    }
  },
  {
    $group: {
      _id: null,
      anos: {
        $push: {
          ano: "$ano",
          emissoes_sp: "$emissoes_sp",
          media_outros: "$media_outros"
        }
      }
    }
  },
  {
    $project: {
      aumento_sp: {
        $divide: [
          {
            $arrayElemAt: [
              "$anos.emissoes_sp",
              { $indexOfArray: ["$anos.ano", 2023] }
            ]
          },
          {
            $arrayElemAt: [
              "$anos.emissoes_sp",
              { $indexOfArray: ["$anos.ano", 1970] }
            ]
          }
        ]
      },
      aumento_medio: {
        $divide: [
          {
            $arrayElemAt: [
              "$anos.media_outros",
              { $indexOfArray: ["$anos.ano", 2023] }
            ]
          },
          {
            $arrayElemAt: [
              "$anos.media_outros",
              { $indexOfArray: ["$anos.ano", 1970] }
            ]
          }
        ]
      }
    }
  },
  {
    $project: {
      aumento_sp: 1,
      aumento_medio: 1,
      aumento_relativo: {
        $cond: [
          { $ne: ["$aumento_medio", 0] },
          { $divide: ["$aumento_sp", "$aumento_medio"] },
          null
        ]
      }
    }
  }
])