var areas = db.estado.aggregate([
    { $unwind: "$municipios" },
    { $unwind: "$municipios.areas" },
    { $match: { "municipios.areas.tipo_area": "RURAL" } },
    { $group: {
        _id: null,
        area_rural: { $sum: "$municipios.areas.tam_area" },
        area_total: { $sum: "$municipios.area_total" }
    }},
    { $project: {
        area_rural: 1,
        area_urbana: { $subtract: ["$area_total", "$area_rural"] }
    }}
]).toArray();

if (areas.length === 0) {
    print("Áreas não encontradas.");
} else {
    var area_rural = areas[0].area_rural;
    var area_urbana = areas[0].area_urbana;

    var emissoes = db.emissao.aggregate([
        { $match: { "origem.setor_origem": { $in: ["Processos Industriais", "Agropecuária"] } } },
        { $group: {
            _id: { ano: "$ano_em", setor: "$origem.setor_origem" },
            emissao: { $sum: "$qtd_em" }
        }},
        { $sort: { "_id.ano": 1 } }
    ]).toArray();

    var resultado_por_ano = {};

    emissoes.forEach(function(emissao) {
        var ano = emissao._id.ano;
        var setor = emissao._id.setor;

        if (!resultado_por_ano[ano]) {
            resultado_por_ano[ano] = { ano: ano, emissao_por_area_rural: 0, emissao_por_area_urbana: 0 };
        }

        if (setor === "Agropecuária") {
            resultado_por_ano[ano].emissao_por_area_rural = area_rural !== 0 ? (emissao.emissao / area_rural) : 0;
        } else if (setor === "Processos Industriais") {
            resultado_por_ano[ano].emissao_por_area_urbana = area_urbana !== 0 ? (emissao.emissao / area_urbana) : 0;
        }
    });

    var resultado = [];

    Object.keys(resultado_por_ano).sort().forEach(function(ano) {
        var dados = resultado_por_ano[ano];
        var proporcao = dados.emissao_por_area_rural !== 0 ? (dados.emissao_por_area_urbana / dados.emissao_por_area_rural) : 0;

        resultado.push({
            ano: dados.ano,
            emissao_por_area_rural: dados.emissao_por_area_rural,
            emissao_por_area_urbana: dados.emissao_por_area_urbana,
            proporcao_urb_rur: proporcao
        });
    });

    printjson(resultado);
}
