db.estado.aggregate([
  { $unwind: "$municipios" },
  { $unwind: "$municipios.areas" },
  {
    $group: {
      _id: "$municipios.areas.tipo_area",
      area_rural: {
        $sum: {
          $cond: [
            { $eq: ["$municipios.areas.tipo_area", "RURAL"] },
            "$municipios.areas.tam_area",
            0
          ]
        }
      },
      area_total: { $sum: "$municipios.area_total" }
    }
  },
  {
    $group: {
      _id: null,
      area_rural: { $sum: "$area_rural" },
      area_total: { $first: "$area_total" }
    }
  },
  {
    $project: {
      area_rural: 1,
      area_urbana: { $subtract: ["$area_total", "$area_rural"] }
    }
  },
  {
    $lookup: {
      from: "emissao",
      pipeline: [
        {
          $match: {
            "origem.setor_origem": { $in: ["Processos Industriais", "Agropecuária"] }
          }
        },
        {
          $group: {
            _id: {
              ano: "$ano_em",
              setor: "$origem.setor_origem"
            },
            emissao: { $sum: "$qtd_em" }
          }
        },
        { $sort: { "_id.ano": 1 } }
      ],
      as: "emissoes"
    }
  },
  {
    $unwind: "$emissoes"
  },
  {
    $project: {
      ano: "$emissoes._id.ano",
      setor: "$emissoes._id.setor",
      emissao: "$emissoes.emissao",
      area_rural: 1,
      area_urbana: 1
    }
  },
  {
    $group: {
      _id: "$ano",
      emissao_por_area_rural: {
        $sum: {
          $cond: [
            { $eq: ["$setor", "Agropecuária"] },
            { $cond: [{ $ne: ["$area_rural", 0] }, { $divide: ["$emissao", "$area_rural"] }, 0] },
            0
          ]
        }
      },
      emissao_por_area_urbana: {
        $sum: {
          $cond: [
            { $eq: ["$setor", "Processos Industriais"] },
            { $cond: [{ $ne: ["$area_urbana", 0] }, { $divide: ["$emissao", "$area_urbana"] }, 0] },
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      ano: "$_id",
      emissao_por_area_rural: 1,
      emissao_por_area_urbana: 1,
      proporcao_urb_rur: {
        $cond: [
          { $ne: ["$emissao_por_area_rural", 0] },
          { $divide: ["$emissao_por_area_urbana", "$emissao_por_area_rural"] },
          0
        ]
      }
    }
  },
  { $sort: { ano: 1 } }
])
