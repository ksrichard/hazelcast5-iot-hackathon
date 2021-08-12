import tensorflow as tf

def transform_list(input_list):
    num_list = [float(it) for it in input_list]
    model = tf.keras.models.Sequential([
        tf.keras.layers.LSTM(units=50, return_sequences=True, input_shape=(len(num_list), 1)),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(units=50, return_sequences=True),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    print(model.summary())
    return num_list
