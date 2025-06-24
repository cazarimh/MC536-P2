var estado = db.estado.findOne({ nome_uf: "SÃO PAULO" });

if (estado) {
    var id_uf = estado._id;

    var total_result = db.emissao.aggregate([
        { $match: { ano_em: 2022, "localizacao.id_uf": id_uf } },
        { $group: { _id: null, total_emissao: { $sum: "$qtd_em" } } }
    ]).toArray();

    var agro_result = db.emissao.aggregate([
        { $match: { ano_em: 2022, "localizacao.id_uf": id_uf, "origem.setor_origem": "Agropecuária" } },
        { $group: { _id: null, emitido_agro: { $sum: "$qtd_em" } } }
    ]).toArray();

    if (total_result.length > 0 && agro_result.length > 0) {
        var total_emissao = total_result[0].total_emissao;
        var emitido_agro = agro_result[0].emitido_agro;
        var porcentagem = (emitido_agro * 100.0) / total_emissao;

        printjson({
            ano: 2022,
            estado: "SÃO PAULO",
            emitido_agro: emitido_agro,
            porcentagem_sobre_total: porcentagem
        });
    } else {
        print("Dados insuficientes para cálculo.");
    }
} else {
    print("Estado não encontrado.");
}
