package bbejeck.util;


public enum Topics {

    COMPANIES {
        @Override
        public String toString() {
            return "companies";
        }
    },
    CLIENTS {
        @Override
        public String toString() {
            return "clients";
        }
    },
    FINANCIAL_NEWS {
        @Override
        public String toString() {
            return "financial-news";
        }
    };


    public String topicName(){
         return this.toString();
    }
}
