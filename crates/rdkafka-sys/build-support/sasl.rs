//! Isolate native SASL adaptations from the shared, revision-checked source cache.

use std::{
    fs,
    path::{Path, PathBuf},
};

fn write_changed(path: &Path, text: &str) {
    if fs::read_to_string(path).ok().as_deref() != Some(text) {
        fs::write(path, text).expect("write private native SASL source");
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create private native source directory");
    for entry in fs::read_dir(source).expect("read native source directory") {
        let entry = entry.expect("read native source entry");
        if entry.file_name() == ".git" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("native source type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).expect("copy private native source");
        }
    }
}

fn replace(source: &str, from: &str, to: &str) -> String {
    assert!(
        source.contains(from),
        "native SASL adaptation no longer matches source: {from}"
    );
    source.replace(from, to)
}

pub fn source(source: &Path, out: &Path, manifest: &Path) -> PathBuf {
    let private = out.join("sasl-native-source");
    let marker = private.join(".rdkafka-rs-source-ready");
    let identity = source.canonicalize().expect("resolve native source path");
    let identity = identity.to_string_lossy();
    if fs::read_to_string(&marker).ok().as_deref() != Some(identity.as_ref()) {
        copy_tree(source, &private);
        fs::write(marker, identity.as_bytes()).expect("mark private native source complete");
    }
    let original = fs::read_to_string(source.join("src/rdkafka_sasl_scram.c"))
        .expect("read upstream SCRAM source");
    let mut scram = replace(
        &original,
        "#if WITH_SSL\n#include <openssl/hmac.h>\n#include <openssl/evp.h>\n#include \
         <openssl/sha.h>\n#else\n#error \"WITH_SSL (OpenSSL) is required for SASL SCRAM\"\n#endif",
        "#include \"sasl_crypto.h\"",
    );
    scram = replace(&scram,
        "        rd_kafka_broker_t *rkb = rktrans->rktrans_rkb;\n        const EVP_MD *evp =\n            rktrans->rktrans_rkb->rkb_rk->rk_conf.sasl.scram_evp;\n        return rd_kafka_ssl_hmac(rkb, evp, in, salt, itcnt, out);",
        "        const EVP_MD *evp = rktrans->rktrans_rkb->rkb_rk->rk_conf.sasl.scram_evp;\n        if (rdkafka_rs_pbkdf2(evp->kind, (const unsigned char *)in->ptr, in->size,\n                              (const unsigned char *)salt->ptr, salt->size,\n                              itcnt, (unsigned char *)out->ptr) != 0)\n                return -1;\n        out->size = evp->size;\n        return 0;");
    scram = replace(
        &scram,
        "static void rd_kafka_sasl_scram_generate_nonce",
        "static int rd_kafka_sasl_scram_generate_nonce",
    );
    scram = replace(&scram,
        "        for (i = 0; i < (int)dst->size; i++)\n                dst->ptr[i] = (char)rd_jitter(0x2d /*-*/, 0x7e /*~*/);\n        dst->ptr[i] = 0;",
        "        if (rdkafka_rs_random((unsigned char *)dst->ptr, dst->size) != 0) {\n                rd_free(dst->ptr);\n                dst->ptr = NULL;\n                dst->size = 0;\n                return -1;\n        }\n        for (i = 0; i < (int)dst->size; i++)\n                dst->ptr[i] = \"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_\"[(unsigned char)dst->ptr[i] & 63];\n        dst->ptr[i] = 0;\n        return 0;");
    scram = replace(
        &scram,
        "static void\nrd_kafka_sasl_scram_build_client_first_message",
        "static int\nrd_kafka_sasl_scram_build_client_first_message",
    );
    scram = replace(
        &scram,
        "        rd_kafka_sasl_scram_generate_nonce(&state->cnonce);",
        "        if (rd_kafka_sasl_scram_generate_nonce(&state->cnonce) != 0)\n                \
         return -1;",
    );
    scram = replace(
        &scram,
        "            rd_memdup(out->ptr + 3, state->first_msg_bare.size);",
        "            rd_memdup(out->ptr + 3, state->first_msg_bare.size);\n        return 0;",
    );
    scram = replace(
        &scram,
        "                rd_kafka_sasl_scram_build_client_first_message(rktrans, &out);",
        "                if (rd_kafka_sasl_scram_build_client_first_message(rktrans, &out) != 0) \
         {\n                        rd_snprintf(errstr, errstr_size, \"SCRAM secure nonce \
         generation failed\");\n                        return -1;\n                }",
    );
    scram = replace(
        &scram,
        "rd_base64_decode(&salt_b64, &salt) == -1",
        "rd_base64_decode(&salt_b64, &salt) != 0",
    );
    write_changed(&private.join("src/rdkafka_sasl_scram.c"), &scram);
    let oauth = fs::read_to_string(source.join("src/rdkafka_sasl_oauthbearer.c"))
        .expect("read upstream OAuth source");
    write_changed(
        &private.join("src/rdkafka_sasl_oauthbearer.c"),
        &replace(
            &oauth,
            "#include <openssl/evp.h>",
            "#include \"sasl_crypto.h\"",
        ),
    );
    let conf = fs::read_to_string(source.join("src/rdkafka_conf.c"))
        .expect("read upstream SASL configuration");
    let conf = replace(
        &conf,
        "#define _UNSUPPORTED_OAUTHBEARER _UNSUPPORTED_SSL",
        "#if WITH_SASL_OAUTHBEARER\n#define _UNSUPPORTED_OAUTHBEARER .unsupported = \
         NULL\n#else\n#define _UNSUPPORTED_OAUTHBEARER .unsupported = \"OAuth provider \
         unavailable\"\n#endif\n#if WITH_SASL_SCRAM\n#define _UNSUPPORTED_SCRAM .unsupported = \
         NULL\n#else\n#define _UNSUPPORTED_SCRAM .unsupported = \"SCRAM provider \
         unavailable\"\n#endif",
    );
    let conf = replace(
        &conf,
        "\"sasl_scram\", _UNSUPPORTED_SSL",
        "\"sasl_scram\", _UNSUPPORTED_SCRAM",
    );
    let conf = replace(
        &conf,
        "\"sasl_oauthbearer\", _UNSUPPORTED_SSL",
        "\"sasl_oauthbearer\", _UNSUPPORTED_OAUTHBEARER",
    );
    write_changed(&private.join("src/rdkafka_conf.c"), &conf);
    let base64 =
        fs::read_to_string(source.join("src/rdbase64.c")).expect("read upstream base64 conversion");
    let base64 = replace(
        &base64,
        "#include \"rdbase64.h\"",
        "#include \"rdbase64.h\"\n#include \"sasl_crypto.h\"",
    );
    let base64 = replace(&base64, "#else\n        return -2;\n#endif", "#else\n        int decoded;\n        if (in->size % 4 != 0 || in->size > INT_MAX) return -1;\n        out->ptr = rd_malloc((in->size / 4) * 3 + 1);\n        decoded = rdkafka_rs_base64_decode((unsigned char *)out->ptr,\n                    (const unsigned char *)in->ptr, in->size);\n        if (decoded < 0) {\n                rd_free(out->ptr);\n                out->ptr = NULL;\n                out->size = 0;\n                return -1;\n        }\n        out->size = (size_t)decoded;\n        return 0;\n#endif");
    write_changed(&private.join("src/rdbase64.c"), &base64);
    let header = fs::read_to_string(manifest.join("build-support/sasl_crypto.h"))
        .expect("read native SASL crypto adapter");
    write_changed(&private.join("src/sasl_crypto.h"), &header);
    private
}
