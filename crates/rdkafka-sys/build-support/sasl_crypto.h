/* Private native SASL adapter. No OpenSSL headers, symbols, or TLS state. */
#ifndef RDKAFKA_RS_SASL_CRYPTO_H
#define RDKAFKA_RS_SASL_CRYPTO_H
#include <stddef.h>
#include <stdint.h>

typedef struct { unsigned int kind; size_t size; } rdkafka_rs_md;
int rdkafka_rs_digest(unsigned int, const unsigned char *, size_t, unsigned char *);
int rdkafka_rs_hmac(unsigned int, const unsigned char *, size_t,
                    const unsigned char *, size_t, unsigned char *);
int rdkafka_rs_pbkdf2(unsigned int, const unsigned char *, size_t,
                      const unsigned char *, size_t, int, unsigned char *);
int rdkafka_rs_random(unsigned char *, size_t);
int rdkafka_rs_base64(unsigned char *, const unsigned char *, int);
int rdkafka_rs_base64_decode(unsigned char *, const unsigned char *, size_t);

static const rdkafka_rs_md rdkafka_rs_sha1_md = {1, 20};
static const rdkafka_rs_md rdkafka_rs_sha256_md = {256, 32};
static const rdkafka_rs_md rdkafka_rs_sha512_md = {512, 64};
static unsigned char *rdkafka_rs_sha1(const unsigned char *d, size_t n, unsigned char *o) {
    return rdkafka_rs_digest(1, d, n, o) == 0 ? o : NULL;
}
static unsigned char *rdkafka_rs_sha256(const unsigned char *d, size_t n, unsigned char *o) {
    return rdkafka_rs_digest(256, d, n, o) == 0 ? o : NULL;
}
static unsigned char *rdkafka_rs_sha512(const unsigned char *d, size_t n, unsigned char *o) {
    return rdkafka_rs_digest(512, d, n, o) == 0 ? o : NULL;
}
static unsigned char *rdkafka_rs_native_hmac(const rdkafka_rs_md *md,
    const unsigned char *key, int key_len, const unsigned char *data,
    size_t len, unsigned char *out, unsigned int *out_len) {
    if (key_len < 0 || rdkafka_rs_hmac(md->kind, key, (size_t)key_len, data, len, out) != 0)
        return NULL;
    if (out_len) *out_len = (unsigned int)md->size;
    return out;
}
#define EVP_MD rdkafka_rs_md
#define EVP_MAX_MD_SIZE 64
#define EVP_sha1() (&rdkafka_rs_sha1_md)
#define EVP_sha256() (&rdkafka_rs_sha256_md)
#define EVP_sha512() (&rdkafka_rs_sha512_md)
#define SHA1 rdkafka_rs_sha1
#define SHA256 rdkafka_rs_sha256
#define SHA512 rdkafka_rs_sha512
#define SHA_DIGEST_LENGTH 20
#define SHA256_DIGEST_LENGTH 32
#define SHA512_DIGEST_LENGTH 64
#define HMAC rdkafka_rs_native_hmac
#define EVP_EncodeBlock rdkafka_rs_base64
#endif
