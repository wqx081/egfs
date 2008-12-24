#include <stdio.h>     /* for printf */
#include <stdlib.h>    /* for exit */
#include <getopt.h>

static char buf[8192] = { 0, };
static char name_buf[1024] = {
    0,
};
static char* file  = "default";
static int   num   = 1;
static int   size  = 0;
static int   begin = 0;

create_file(char* name, int size) {
    FILE* fp = fopen(name, "w");
    int left = size;
    while (left > 0) {
        int write_cnt = fwrite(buf, 1, (left > 8192) ? 8192 : left, fp);
        left -= write_cnt;
    }
    fclose(fp);
}

int main(int argc, char **argv)
{
    int c;
    int digit_optind = 0;

    while (1) {
        int this_option_optind = optind ? optind : 1;
        int option_index = 0;
        static struct option long_options[] = {
            {"file", 1, 0, 'f'},
            {"num", 1, 0, 'n'},
            {"begin", 1, 0, 'b'},
            {"size", 1, 0, 's'},
            {0, 0, 0, 0}
        };

        c = getopt_long(argc, argv, "n:f:s:b:",
                        long_options, &option_index);
        if (c == -1)
            break;

        switch (c) {
            case 'f':
                file = optarg;
                break;

            case 'n':
                num = atoi(optarg);
                break;
            case 'b':
                begin = atoi(optarg);
                break;
            case 's':
                size = atoi(optarg);
                break;

            case '?':
                break;

            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }
    }
    
    int cnt = begin;
    for(; num > 0; num--) {
        snprintf(name_buf, 1024, "%s%d", file, cnt++);
        printf("generate file: %s,\tsize: %d ...", name_buf, size);
        fflush(stdout);
        create_file(name_buf, size);
        printf("\t done!\n");
    }

}
