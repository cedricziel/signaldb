//! A throwaway RSA private key for GitHub App tests (change:
//! github-app-source-context). Generated once with
//! `openssl genrsa -traditional 2048`; PKCS#1 `RSA PRIVATE KEY` is the form
//! GitHub issues. Shared by the `[github]` config tests, the router's GitHub
//! client tests, and its endpoint tests so the key lives in exactly one
//! place.

/// PEM-encoded PKCS#1 RSA private key used only by tests.
pub const GITHUB_TEST_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA2qU4T+0ou5uiRMCaLXCeONctb6Xv7k9qcrGd1srIhfMzOD0S
T70/ulP0n88rnsjaShTN2NB8FDPXZzzq6ssDObDAa9EYvoDW88teo6VkNVUzZE+f
E6jpsD4jmRSOyDP6kgL+6E6F3lwWJFJ90iEQpaf7/fzt553BcAIzI18sI06ytfLh
K+q/FP9g4EqUUfF37IYea/NDILwr9bzfgA6/txoZuP5+XBToiJNpedXTn9mSX+qk
AGrB4Ki7hyTE7XLnMQbeILCeleNb7HxwXvMg4lE/JI/4YopZFmBxoWMOrw6+CvuK
LtsJ3sANAKg288VfEyX9/7T7kFRzk92PtT1okQIDAQABAoIBACJLj2/8q3VJ7dj0
5k9sU7+8rgl422U48GHPJF/gQjW94PHYT8/8HPaKsVsucWbK6xp69ZLQQ4tzr5In
nFLPHHwRvQzsU6zwo/Fs8TT1U1OI6rJi9CPETJYdbmirhdM3UVKnxPixSw7cHjOn
ph6t8GDbiYuG/ytjs5zFwORdIEeP3VDYko9p2SiDkZEaUvE2qSqK6RL0wsOh7Xdc
ODNscGsfzLFbVi6t6+3fLFAg5fj57Hm0Ftz9tvSSSMj/OidnjOW39Vd0i8yFLw5A
XxMKAJRdAqCglvEdb0+foyrlwPda9FiDXAjUspYc3FCs7PS5Pcnn7zBeWfRLNZxn
2ndY/mUCgYEA8irhEGincROi12lFMypxkANXLbNYloK8X/VxQARizKOqs2KHhhJX
Fk3iSd44ZojvNRMuCubmRiecTo2Ua+VK8d2zdBmmewTr+IZbXnlJgXFFZoxFJwIR
2fxOy6uI4LVta1wM00uF6kw7KgqGt9kUdVHRS0A+gRR0DYabMyCEFosCgYEA5yJi
3+mpi1vNIdMZsx1y0FSZQ1Z2p3XtxcbCbHsy/Rs3WtHzyh/zZgDwJDL/jUSblVVs
GqEWFDd2EN3FbhfC6Kr0511uyMyvejmCDYD+B8c9Q2HIBMbbOFdVOZ3eu2ZJR6Ox
FMUD1guUo80RNeFIlFc5Sy7khOTkAxQBO9pK/NMCgYEA1ISOuGCvOMubp2Cpqso/
mLtlwSRXxNX8TFXPyfdPYPjsb7oy5pSnulolEOAkLM9U5QXs7QJO6RgP0tvqeXli
eLkvp95uvBzInHQEMPdNa3wlBGZqtV3anqsN1yy01UZCPOouEyt+3OuDLFTKfwLR
MlfXzSsW7x4s7kXHY2nQoRsCgYANTZFbSduURchctgW4pW1CSFw53/QcV1FEHNh7
3etlXfelpofdjlE4Ab3Ql47V1qkNw0jhj3vx1e9ZsSn32C5DrfNCjcIelIlVA5JU
rKPyVqlUA4C7paZga8Zf9pInPw/gq685fs1luGzpsJfY/gprX0nQ5fYJIBNvies4
QqwmOQKBgCLNwlOMk4awQwzMCBvqivEC4KthWZv3vGf+2gJFKvxb4uZGqn2U3Rhp
CngpDDWPWGGiGKKEcm/ewxC4FLTcgLIacuQebCnMZIin79dOVxm8pzwTBWWmvdqu
GAqdwtFpRd3NeFgeYhShbOJlZIGl6uEhCSqYDZ1UjXx5+kSbBOMr
-----END RSA PRIVATE KEY-----";
