module github.com/kubeflow/pipelines

require (
	github.com/Masterminds/squirrel v0.0.0-20190107164353-fa735ea14f09
	github.com/VividCortex/mysqlerr v0.0.0-20170204212430-6c6b55f8796f
	github.com/argoproj/argo-workflows/v3 v3.5.8
	github.com/aws/aws-sdk-go v1.45.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/eapache/go-resiliency v1.2.0
	github.com/erikstmartin/go-testdb v0.0.0-20160219214506-8d10e4a1bae5 // indirect
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-openapi/errors v0.20.2
	github.com/go-openapi/runtime v0.21.1
	github.com/go-openapi/strfmt v0.21.1
	github.com/go-openapi/swag v0.22.3
	github.com/go-openapi/validate v0.20.3
	github.com/go-sql-driver/mysql v1.7.1
	github.com/golang/glog v1.1.0
	github.com/golang/protobuf v1.5.4
	github.com/google/addlicense v0.0.0-20200906110928-a0294312aa76
	github.com/google/cel-go v0.9.0
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/jackc/pgx/v5 v5.4.2
	github.com/jinzhu/gorm v1.9.1
	github.com/kubeflow/pipelines/api v0.0.0-20230331215358-758c91f76784
	github.com/kubeflow/pipelines/kubernetes_platform v0.0.0-20240403164522-8b2a099e8c9f
	github.com/kubeflow/pipelines/third_party/ml-metadata v0.0.0-20230810215105-e1f0c010f800
	github.com/lestrrat-go/strftime v1.0.4
	github.com/mattn/go-sqlite3 v1.14.18
	github.com/minio/minio-go/v6 v6.0.57
	github.com/peterhellberg/duration v0.0.0-20191119133758-ec6baeebcd10
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.16.0
	github.com/prometheus/client_model v0.4.0
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/viper v1.17.0
	github.com/stretchr/testify v1.9.0
	gocloud.dev v0.22.0
	golang.org/x/net v0.26.0
	google.golang.org/genproto v0.0.0-20231016165738-49dd2c1f3d0b // indirect
	google.golang.org/grpc v1.59.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.33.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.24.17
	k8s.io/apimachinery v0.24.17
	k8s.io/client-go v0.24.3
	k8s.io/code-generator v0.23.5
	sigs.k8s.io/controller-runtime v0.11.2
	sigs.k8s.io/yaml v1.4.0
)

require (
	google.golang.org/genproto/googleapis/api v0.0.0-20231016165738-49dd2c1f3d0b
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231030173426-d783a09b4405
)

require (
	cloud.google.com/go v0.110.8 // indirect
	cloud.google.com/go/compute v1.23.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.3 // indirect
	cloud.google.com/go/storage v1.36.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.2.0 // indirect
	github.com/Masterminds/sprig/v3 v3.2.3 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20210826220005-b48c857c3a0e // indirect
	github.com/argoproj/pkg v0.13.7-0.20230901113346-235a5432ec98 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/colinmarc/hdfs/v2 v2.4.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/doublerebel/bellows v0.0.0-20160303004610-f177d92a03d3 // indirect
	github.com/elazarl/goproxy v0.0.0-20230808193330-2592e75ae04a // indirect
	github.com/emicklei/go-restful/v3 v3.10.0 // indirect
	github.com/evanphx/json-patch v5.8.0+incompatible // indirect
	github.com/evilmonkeyinc/jsonpath v0.8.1 // indirect
	github.com/expr-lang/expr v1.16.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-openapi/analysis v0.21.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/loads v0.21.0 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/wire v0.4.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/gorilla/websocket v1.5.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/huandu/xstrings v1.3.3 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/onsi/ginkgo/v2 v2.11.0 // indirect
	github.com/onsi/gomega v1.27.10 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/sagikazarmark/locafero v0.3.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.10.0 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	go.mongodb.org/mongo-driver v1.8.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/oauth2 v0.13.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/term v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	golang.org/x/time v0.4.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	gomodules.xyz/jsonpatch/v2 v2.2.0 // indirect
	google.golang.org/api v0.151.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apiextensions-apiserver v0.23.5 // indirect
	k8s.io/component-base v0.24.3 // indirect
	k8s.io/gengo v0.0.0-20220613173612-397b4ae3bce7 // indirect
	k8s.io/klog/v2 v2.70.1 // indirect
	k8s.io/kube-openapi v0.0.0-20220627174259-011e075b9cb8 // indirect
	k8s.io/utils v0.0.0-20220713171938-56c0de1e6f5e // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
)

replace (
	github.com/argoproj/argo-events v0.17.1-0.20220223155401-ddda8800f9f8 => github.com/argoproj/argo-events v1.7.1
	github.com/cloudevents/sdk-go/v2 v2.10.0 => github.com/cloudevents/sdk-go/v2 v2.15.1
	github.com/cloudflare/circl v1.3.3 => github.com/cloudflare/circl v1.3.7
	github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/dgrijalva/jwt-go/v4 v4.0.0-preview1
	github.com/gin-gonic/gin => github.com/gin-gonic/gin v1.9.1
	github.com/go-git/go-git/v5 => github.com/go-git/go-git/v5 v5.11.0
	github.com/go-jose/go-jose/v3 => github.com/go-jose/go-jose/v3 v3.0.1
	github.com/jackc/pgx/v5 v5.4.2 => github.com/jackc/pgx/v5 v5.5.4
	github.com/labstack/echo v3.2.1+incompatible => github.com/labstack/echo/v4 v4.2.0
	github.com/nats-io/nats-server/v2 v2.7.2 => github.com/nats-io/nats-server/v2 v2.10.2
	github.com/nats-io/nkeys v0.4.5 => github.com/nats-io/nkeys v0.4.6
	go.opentelemetry.io/contrib v0.20.0 => go.opentelemetry.io/contrib v1.27.0
	google.golang.org/grpc => google.golang.org/grpc v1.56.3
	nhooyr.io/websocket v1.8.6 => nhooyr.io/websocket v1.8.7
)

go 1.21
